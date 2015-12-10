from abc import ABCMeta, abstractmethod

from execo_engine import logger


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
    master = None

    @staticmethod
    def get_cluster_type():
        """Returns a str with the type of the cluster."""
        pass

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

    def _check_initialization(self):
        """ Check whether the cluster is initialized and raise and exception if
        not.
        """

        if not self.initialized:
            logger.error("The cluster should be initialized")
            raise ClusterNotInitializedException(
                "The cluster should be initialized")

    @abstractmethod
    def change_conf(self, params, config_file=None, default_file=None):
        pass

    @abstractmethod
    def get_conf(self, param_names, node=None):
        pass

    @abstractmethod
    def get_conf_param(self, param_name, default_value=None, node=None):
        pass

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


class ClusterException(Exception):
    pass


class ClusterNotInitializedException(ClusterException):
    pass
