import os
import shutil
import tempfile
from execo import SshProcess
import time
import yaml

from subprocess import call
from yaml import CLoader as Loader, CDumper as Dumper

from execo.action import TaktukPut, Remote, TaktukRemote, SequentialActions
from execo.log import style
from execo_engine import logger

from bigdata_dpy.base.cluster import BaseCluster


# Configuration files
CONF_FILE = "mongodb.conf"

# Default parameters
DEFAULT_MONGODB_BASE_DIR = "/tmp/mongodb"
DEFAULT_MONGODB_DATA_DIR = DEFAULT_MONGODB_BASE_DIR + "/data"
DEFAULT_MONGODB_CONF_DIR = DEFAULT_MONGODB_BASE_DIR + "/conf"
DEFAULT_MONGODB_LOGS_DIR = DEFAULT_MONGODB_BASE_DIR + "/logs"

DEFAULT_MONGOD_PORT = 27017
DEFAULT_MONGOS_PORT = 27019

DEFAULT_MONGODB_LOCAL_CONF_DIR = "mongo-conf"

# Other
NUMA_PREFIX = "numactl --interleave=all"


class MongoDBCluster(BaseCluster):
    """This class manages the whole life-cycle of a MongoDB cluster.
    """

    @staticmethod
    def get_cluster_type():
        return "mongodb"

    def get_default_conf(self):
        ctype = self.get_cluster_type()

        defaults = {
            ctype + "_base_dir": DEFAULT_MONGODB_BASE_DIR,
            ctype + "_data_dir": DEFAULT_MONGODB_DATA_DIR,
            ctype + "_conf_dir": DEFAULT_MONGODB_CONF_DIR,
            ctype + "_logs_dir": DEFAULT_MONGODB_LOGS_DIR,
            ctype + "_md_port": str(DEFAULT_MONGOD_PORT),
            ctype + "_ms_port": str(DEFAULT_MONGOS_PORT),

            ctype + "_local_base_conf_dir": DEFAULT_MONGODB_LOCAL_CONF_DIR
        }
        return defaults

    def __init__(self, hosts, config_file=None,
                 sharding=True, replication=False):
        """Create a new MongoDB cluster with the given hosts.

        Args:
          hosts (list of Host):
            The hosts that conform the cluster.
          config_file (str, optional):
            The path of the config file to be used.
        """

        super(MongoDBCluster, self).__init__(hosts, config_file)

        # Cluster properties
        ctype = self.get_cluster_type()
        self.data_dir = self.config.get("cluster", ctype + "_data_dir")
        self.logs_dir = self.config.get("cluster", ctype + "_logs_dir")
        self.md_port = self.config.getint("cluster", ctype + "_md_port")
        self.ms_port = self.config.getint("cluster", ctype + "_ms_port")
        self.bin_dir = self.base_dir + "/bin"
        self.conf_mandatory_files = [CONF_FILE]

        # Configure master
        self.master = hosts[0]

        self.do_sharding = sharding
        self.initialized_sharding = False
        self.mongos_pid_file = self.base_dir + "/mongos.pid"
        self.do_replication = replication
        # TODO: allow more fine-frained configuration of hosts: assign roles

        logger.info("MongoDB cluster created with master %s and hosts %s %s "
                    "replication, %s sharding",
                    style.host(self.master.address),
                    ' '.join([style.host(h.address.split('.')[0])
                              for h in self.hosts]),
                    "with" if self.do_replication else "without",
                    "with" if self.do_sharding else "without")

    def bootstrap(self, tar_file):
        """Install MongoDB in all cluster nodes from the specified tgz file.

        Args:
          tar_file (str):
            The file containing MongoDB binaries.
        """

        # 1. Copy mongo tar file and uncompress
        logger.info("Copy " + tar_file + " to hosts and uncompress")
        rm_files = TaktukRemote("rm -rf " + self.base_dir +
                                " " + self.conf_dir +
                                " " + self.data_dir +
                                " " + self.logs_dir,
                                self.hosts)

        put_tar = TaktukPut(self.hosts, [tar_file], "/tmp")
        tar_xf = TaktukRemote("tar xf /tmp/" + os.path.basename(tar_file) +
                              " -C /tmp", self.hosts)
        SequentialActions([rm_files, put_tar, tar_xf]).run()

        # 2. Move installation to base dir
        logger.info("Create installation directories")
        action = Remote(
            "mv /tmp/" +
            os.path.basename(tar_file).replace(".tgz", "") + " " +
            self.base_dir,
            self.hosts)
        action.run()

        # 3 Create other dirs
        mkdirs = TaktukRemote("mkdir -p " + self.data_dir +
                              " && mkdir -p " + self.conf_dir +
                              " && mkdir -p " + self.logs_dir +
                              " && touch " + os.path.join(self.conf_dir,
                                                          CONF_FILE),
                              self.hosts)
        mkdirs.run()

        # 4. Generate initial configuration
        self._initialize_conf()

    def initialize(self, default_tuning=False):
        """Initialize the cluster: copy base configuration."""

        self._pre_initialize()

        logger.info("Initializing MongoDB")

        # Set basic configuration
        temp_conf_base_dir = tempfile.mkdtemp("", "mongo-", "/tmp")
        temp_conf_dir = os.path.join(temp_conf_base_dir, "conf")
        shutil.copytree(self.init_conf_dir, temp_conf_dir)

        self._create_master_and_slave_conf(temp_conf_dir)
        self._configure_servers(temp_conf_dir, default_tuning)

        shutil.rmtree(temp_conf_base_dir)

        self.initialized = True

    def _pre_initialize(self):
        """Clean previous configurations"""

        if self.initialized:
            if self.running:
                self.stop()
            self.clean()
        else:
            self.__force_clean()

        self.initialized = False

    def _create_master_and_slave_conf(self, conf_dir):
        """Create master and slaves configuration files."""

        # Load configuration
        conf_file = os.path.join(conf_dir, CONF_FILE)
        with open(conf_file) as conf_stream:
            config = yaml.load(conf_stream, Loader=Loader)
            if not config:
                config = {}

        # General configuration
        if "systemLog" not in config:
            config["systemLog"] = {}
        config["systemLog"]["destination"] = "file"
        config["systemLog"]["path"] = self.logs_dir + "/mongod.log"

        if "storage" not in config:
            config["storage"] = {}
        config["storage"]["dbPath"] = self.data_dir

        # Replication
        if self.do_replication or self.do_sharding:
            self.rs_name = "mdb_" + str(self.master.address)
            if "replication" not in config:
                config["replication"] = {}
            rep_config = config["replication"]

            rep_config["replSetName"] = self.rs_name

        # Sharding
        if self.do_sharding:
            if "sharding" not in config:
                config["sharding"] = {}
            sh_config = config["sharding"]
            sh_config["clusterRole"] = "configsvr"

        # Write back configuration
        with open(conf_file, "w") as conf_stream:
            conf_stream.write(yaml.dump(config, Dumper=Dumper))

    def _configure_servers(self, conf_dir, default_tuning=False):
        self._copy_conf(conf_dir)

    def _copy_conf(self, conf_dir, hosts=None):

        if not hosts:
            hosts = self.hosts

        conf_files = [os.path.join(conf_dir, f) for f in os.listdir(conf_dir)]

        action = TaktukPut(hosts, conf_files, self.conf_dir)
        action.run()

        if not action.finished_ok:
            logger.warn("Error while copying configuration")
            if not action.ended:
                action.kill()

    def start(self):
        """Start MongoDB server."""

        self._check_initialization()

        logger.info("Starting MongoDB")

        if self.running:
            logger.warn("MongoDB was already started")
            return

        # Start nodes
        procs = []
        for h in self.hosts:
            mongo_command = (NUMA_PREFIX + " " +
                             self.bin_dir + "/mongod "
                             " --fork "
                             " --config " + os.path.join(self.conf_dir,
                                                         CONF_FILE) +
                             " --bind_ip " + h.address +
                             " --port " + str(self.md_port))

            logger.debug(mongo_command)

            proc = SshProcess(mongo_command, h)
            proc.start()
            procs.append(proc)

        finished_ok = True
        for p in procs:
            p.wait()
            if not p.finished_ok:
                finished_ok = False

        if not finished_ok:
            logger.warn("Error while starting MongoDB")
            return
        else:
            self.running = True

        # Start replication
        if self.do_replication:
            logger.info("Configuring replication")
            mongo_command = "rs.initiate();"
            mongo_command += ';'.join(
                'rs.add("' + h.address + ':' + str(self.md_port) + '")'
                for h in self.hosts)

            logger.debug(mongo_command)

            proc = TaktukRemote(self.bin_dir + "/mongo "
                                "--eval '" + mongo_command + "' " +
                                self.master.address,
                                [self.master])
            proc.run()

            if not proc.finished_ok:
                logger.warn("Not able to start replication")

        if self.do_sharding:
            if not self.initialized_sharding:
                logger.info("Configuring sharding")
                time.sleep(2)
                mongo_command = (
                    'rs.initiate({'
                    '_id : "%s",'
                    'configsvr : true,'
                    'members : [%s]})' % (
                        self.rs_name,
                        ",".join('{ _id : %d, host : "%s:%d" }' %
                                 (_id, h.address, self.md_port)
                                 for (_id, h) in enumerate(self.hosts))
                    )
                )

                logger.debug(mongo_command)

                proc = SshProcess(self.bin_dir + "/mongo " +
                                  "--eval '" + mongo_command + "' " +
                                  self.master.address,
                                  self.master)
                proc.run()
                if proc.finished_ok:
                    self.initialized_sharding = True
                else:
                    logger.warn("Not able to configure sharding")

            logger.info("Starting sharding servers")
            mongo_command = (
                NUMA_PREFIX + " " +
                self.bin_dir + "/mongos"
                " --configdb " + self.rs_name + "/" +
                ",".join('%s:%d' % (h.address, self.md_port)
                         for h in self.hosts) +
                " --bind_ip " + self.master.address +
                " --port " + str(self.ms_port) +
                " --fork"
                " --logpath " + self.logs_dir + "/mongos.log"
                " --pidfilepath " + self.mongos_pid_file
            )

            logger.debug(mongo_command)

            start_ms = TaktukRemote(mongo_command, [self.master])
            start_ms.run()

    def start_shell(self, node=None, mongos=True):
        """Open a MongoDB shell.

        Args:
          node (Host, optional):
            The host were the shell is to be started. If not provided,
            self.master is chosen.
        """

        self._check_initialization()

        if not node:
            node = self.master

        if mongos and self.do_sharding:
            port = self.ms_port
        else:
            port = self.md_port

        mongo_command = (
            self.bin_dir + "/mongo"
            " --host " + node.address +
            " --port " + str(port)
        )

        logger.debug(mongo_command)

        call("ssh -t " + node.address + " " +
             NUMA_PREFIX + " " + mongo_command,
             shell=True)

    def stop(self):
        """Stop MongoDB servers."""

        self._check_initialization()

        logger.info("Stopping MongoDB")

        proc = TaktukRemote(self.bin_dir + "/mongod "
                            "--shutdown "
                            "--dbpath " + self.data_dir,
                            self.hosts)
        proc.run()

        proc = TaktukRemote("kill $(more " + self.mongos_pid_file + ")",
                            [self.master])
        proc.run()

        self.running = False

    def clean_logs(self):
        """Remove all MongoDB logs."""

        logger.info("Cleaning logs")

        restart = False
        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()
            restart = True

        action = Remote("rm -rf " + self.logs_dir, self.hosts)
        action.run()

        if restart:
            self.start()

    def clean_data(self):
        """Remove all data created by Hadoop (including filesystem)."""

        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()

        logger.info("Cleaning MongoDB data")

        restart = False
        if self.running:
            self.stop()
            restart = True

        action = Remote("rm -rf " + self.data_dir + "/*", self.hosts)
        action.run()

        if restart:
            self.start()

    def clean(self):
        """Remove all files created by MongoDB."""

        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()

        self.clean_logs()
        self.clean_data()

    def __force_clean(self):
        pass

