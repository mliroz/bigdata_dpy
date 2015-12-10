import os
import sys

from ConfigParser import ConfigParser

from execo.action import Remote, Put
from execo.log import style
from execo.process import SshProcess
from execo_engine import logger

from bigdata_dpy.util import ColorDecorator

# Default parameters
DEFAULT_MAHOUT_BASE_DIR = "/tmp/mahout"
DEFAULT_MAHOUT_CONF_DIR = DEFAULT_MAHOUT_BASE_DIR + "/conf"
DEFAULT_MAHOUT_BIN_DIR = DEFAULT_MAHOUT_BASE_DIR + "/bin"


class MahoutCluster(object):

    @staticmethod
    def get_cluster_type():
        return "mahout"

    initialized = False

    # Default properties
    defaults = {
        "mahout_base_dir": DEFAULT_MAHOUT_BASE_DIR,
        "mahout_conf_dir": DEFAULT_MAHOUT_CONF_DIR
    }

    def __init__(self, hadoop_cluster, config_file=None):

        # Load cluster properties
        config = ConfigParser(self.defaults)
        config.add_section("cluster")
        config.add_section("local")

        if config_file:
            config.readfp(open(config_file))

        self.base_dir = config.get("cluster", "mahout_base_dir")
        self.conf_dir = config.get("cluster", "mahout_conf_dir")

        self.bin_dir = self.base_dir + "/bin"

        self.hc = hadoop_cluster

        # Create topology
        logger.info("Mahout cluster created in hosts %s",
                    ' '.join([style.host(h.address.split('.')[0])
                              for h in self.hc.hosts]))

    def bootstrap(self, tar_file):

        # 1. Remove used dirs if existing
        action = Remote("rm -rf " + self.base_dir, self.hc.hosts)
        action.run()
        action = Remote("rm -rf " + self.conf_dir, self.hc.hosts)
        action.run()

        # 1. Copy Mahout tar file and uncompress
        logger.info("Copy " + tar_file + " to hosts and uncompress")
        action = Put(self.hc.hosts, [tar_file], "/tmp")
        action.run()
        action = Remote(
            "tar xf /tmp/" + os.path.basename(tar_file) + " -C /tmp",
            self.hc.hosts)
        action.run()

        # 2. Move installation to base dir
        logger.info("Create installation directories")
        action = Remote(
            "mv /tmp/" +
            os.path.basename(tar_file).replace(".tar.gz", "") + " " +
            self.base_dir,
            self.hc.hosts)
        action.run()

        # 3 Create other dirs
        action = Remote("mkdir -p " + self.conf_dir, self.hc.hosts)
        action.run()

        # 4. Include libraries in Hadoop's classpath
        list_dirs = SshProcess("ls -1 " + self.base_dir + "/*.jar",
                               self.hc.master)
        list_dirs.run()
        libs = " ".join(list_dirs.stdout.splitlines())
        action = Remote("cp " + libs + " " + self.hc.base_dir + "/lib",
                        self.hc.hosts)
        action.run()

        initialized = True  # No need to call initialize()

    def initialize(self):
        pass

    def execute(self, command, node=None, verbose=True):

        if not node:
            node = self.hc.master

        if verbose:
            logger.info("Executing {" + self.bin_dir + "/mahout " +
                        command + "} in " + str(node))

        proc = SshProcess("export JAVA_HOME='" + self.hc.java_home + "';" +
                          "export HADOOP_HOME='" + self.hc.base_dir + "';" +
                          self.bin_dir + "/mahout " + command, node)

        if verbose:
            red_color = '\033[01;31m'

            proc.stdout_handlers.append(sys.stdout)
            proc.stderr_handlers.append(ColorDecorator(sys.stderr, red_color))

        proc.start()
        proc.wait()

        return proc.stdout, proc.stderr

    def clean(self):
        pass
