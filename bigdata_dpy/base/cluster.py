from ConfigParser import ConfigParser
from abc import ABCMeta, abstractmethod
import os
import tempfile
from execo import Get

from execo_engine import logger
import shutil
from bigdata_dpy.util import hw_manager


class Cluster(object):

    __metaclass__ = ABCMeta

    # Cluster state
    initialized = False
    running = False

    # General properties
    base_dir = None
    local_base_conf_dir = None

    # Nodes
    hosts = []

    @staticmethod
    def get_cluster_type():
        """Returns a str with the type of the cluster."""
        return ""

    @abstractmethod
    def bootstrap(self, dist_file):
        """Install the software in all cluster nodes from the specified file.

        Args:
          dist_file (str):
            The file containing the software binaries or sources.
        """
        pass

    @abstractmethod
    def initialize(self, default_tuning=False):
        """Initialize the cluster."""
        self.initialized = True

    @abstractmethod
    def start(self):
        """Start the server"""
        self.running = True

    @abstractmethod
    def stop(self):
        """Stop the server."""
        self.running = False

    @abstractmethod
    def clean(self):
        """Remove files created during cluster operation and return back to the
        non-initialized state."""
        self.initialized = False

    def __str__(self):
        if self.initialized:
            if self.running:
                state = "running"
            else:
                state = "initialized"
        else:
            state = "not initialized"

        node_info = str(len(self.hosts))

        return "%s([%s], %s)" % (
            type(self).__name__,
            node_info,
            state
        )


class BaseCluster(Cluster):

    __metaclass__ = ABCMeta

    # Configuration
    config = None

    conf_dir = None
    conf_mandatory_files = []

    def __init__(self, hosts, config_file=None):

        ctype = self.get_cluster_type()

        # Load properties
        self.config = ConfigParser(self.get_default_conf())
        self.config.add_section("cluster")
        self.config.add_section("local")

        if config_file:
            self.config.readfp(open(config_file))

        # General properties
        self.local_base_conf_dir = \
            self.config.get("local", ctype + "_local_base_conf_dir")
        self.init_conf_dir = tempfile.mkdtemp("", ctype + "-init-", "/tmp")

        self.base_dir = self.config.get("cluster", ctype + "_base_dir")
        self.conf_dir = self.config.get("cluster", ctype + "_conf_dir")

        # Cluster hosts
        self.hosts = list(hosts)

    @abstractmethod
    def get_default_conf(self):
        """Returns a dict with the default configuration."""
        return {}

    def _initialize_conf(self):
        """Merge locally-specified configuration files with default files
        from the distribution."""

        if os.path.exists(self.local_base_conf_dir):
            base_conf_files = [os.path.join(self.local_base_conf_dir, f)
                               for f in os.listdir(self.local_base_conf_dir)]
            for f in base_conf_files:
                shutil.copy(f, self.init_conf_dir)
        else:
            logger.warn(
                "Local conf dir does not exist. Using default configuration")
            base_conf_files = []

        missing_conf_files = self.conf_mandatory_files
        for f in base_conf_files:
            f_base_name = os.path.basename(f)
            if f_base_name in missing_conf_files:
                missing_conf_files.remove(f_base_name)

        logger.info("Copying missing conf files from master: " + str(
            missing_conf_files))

        remote_missing_files = [os.path.join(self.conf_dir, f)
                                for f in missing_conf_files]

        action = Get([self.hosts[0]], remote_missing_files, self.init_conf_dir)
        action.run()

    def _check_initialization(self):
        """ Check whether the cluster is initialized and raise and exception if
        not.
        """

        if not self.initialized:
            logger.error("The cluster should be initialized")
            raise ClusterNotInitializedException(
                "The cluster should be initialized")

    @abstractmethod
    def _configure_servers(self, conf_dir, default_tuning=False):
        pass


class HWBasedConfigured(object):

    hw = None

    def __init__(self, hosts):
        self.hw = hw_manager.make_deployment_hardware()
        self.hw.add_hosts(list(hosts))

    def _configure_servers(self, conf_dir, default_tuning=False):
        """Configure servers and host-dependant parameters.

           Args:
             conf_dir (str):
               The path of the directory with the configuration files.
             default_tuning (bool, optional):
               Whether to use automatic tuning based on some best practices or
               leave the default parameters.
        """

        if default_tuning:
            logger.info("Default tuning. Beware that this configuration is not"
                        "guaranteed to be optimal for all scenarios.")

        # Get cluster-dependent params
        params = self._get_cluster_params(conf_dir, default_tuning)
        logger.info("Params = " + str(params))

        # Set common configuration
        self._set_common_params(params, conf_dir, default_tuning)

        # Set cluster-dependent configuration and copy back to hosts
        for cluster in self.hw.get_clusters():

            # Create a new dir
            cl_temp_conf_base_dir = tempfile.mkdtemp("", "hadoop-cl-", "/tmp")
            cl_temp_conf_dir = os.path.join(cl_temp_conf_base_dir, "conf")
            shutil.copytree(conf_dir, cl_temp_conf_dir)

            # Replace params in conf files
            self._set_cluster_params(cluster, params, cl_temp_conf_dir,
                                     default_tuning)

            # Copy to hosts and remove temp dir
            hosts = cluster.get_hosts()
            self._copy_conf(cl_temp_conf_dir, hosts)
            shutil.rmtree(cl_temp_conf_base_dir)

    @abstractmethod
    def _get_cluster_params(self, conf_dir, default_tuning):
        return {}

    @abstractmethod
    def _set_common_params(self, params, conf_dir, default_tuning):
        pass

    @abstractmethod
    def _set_cluster_params(self, cluster, params, cl_temp_conf_dir,
                            default_tuning):
        pass

    @abstractmethod
    def _copy_conf(self, cl_temp_conf_dir, hosts):
        pass


class Configuration(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def change_conf(self, params, config_file=None, default_file=None):
        pass

    @abstractmethod
    def get_conf(self, param_names, node=None):
        pass

    @abstractmethod
    def get_conf_param(self, param_name, default_value=None, node=None):
        pass


class ClusterException(Exception):
    pass


class ClusterNotInitializedException(ClusterException):
    pass
