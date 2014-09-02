#!/usr/bin/env python


import datetime
import os
import sys
import time
import ConfigParser

from hadoop_g5k import HadoopCluster

from execo.action import Remote
from execo.time_utils import timedelta_to_seconds, format_date
from execo_g5k import get_cluster_site, get_oar_job_info, oardel, oarsub, \
    get_planning, compute_slots, get_jobs_specs, get_oar_job_nodes, \
    deploy, Deployment, get_current_oar_jobs
from execo_engine import Engine, logger, sweep, ParamSweeper

DEFAULT_DATA_BASE_DIR = "/tests/data"
DEFAULT_OUT_BASE_DIR = "/tests/out"

class HadoopEngine(Engine):

    def __init__(self):
        self.frontend = None
        super(HadoopEngine, self).__init__()
        
        self.options_parser.set_usage("usage: %prog <cluster> <n_nodes> <config_file>")
        self.options_parser.add_argument("cluster",
                    "The cluster on which to run the experiment")
        self.options_parser.add_argument("n_nodes",
                    "The number of nodes in which the experiment is going to be deployed")
        self.options_parser.add_argument("param_file",
                    "The path of the file containing the test params (INI file)")                             
        self.options_parser.add_option("-k", dest="keep_alive",
                    help="keep reservation alive ..",
                    action="store_true")
        self.options_parser.add_option("-j", dest="oar_job_id",
                    help="oar_job_id to relaunch an engine",
                    type=int)
        self.options_parser.add_option("-o", dest="outofchart",
                    help="Run the engine outside days",
                    action="store_true")
        self.options_parser.add_option("-w", dest="walltime",
                    help="walltime for the reservation",
                    type="string",
                    default="1:00:00")
                    
        self.hc = None
        
        self.macros = {
           "${data_base_dir}" : "/tests/data",
           "${out_base_dir}" : "/tests/out",
           "${data_dir}" : "/tests/data/0", # ${data_base_dir}/${ds_id}
           "${out_dir}" : "/tests/out/0", # ${data_base_dir}/${comb_id}
           "${comb_id}" : 0,
           "${ds_id}" : 0,
           "${xp.input}" : "/tests/data/0", # ${data_dir}
           "${xp.output}" : "/tests/out/0" # ${out_dir}
        }
        
        self.comb_id = 0
        self.ds_id = 0
        
        
    def __update_macros(self):
        self.macros["${data_dir}"] = self.macros["${data_base_dir}"] + "/" + str(self.ds_id)
        self.macros["${out_dir}"] = self.macros["${out_base_dir}"] + "/" + str(self.comb_id)
        self.macros["${xp.input}"] = self.macros["${data_dir}"]
        self.macros["${xp.output}"] = self.macros["${out_dir}"]
        

    def run(self):
        """Inherited method, put here the code for running the engine"""

        # Get parameters
        self.cluster = self.args[0]
        self.n_nodes = int(self.args[1])
        self.config_file = self.args[2]
        self.site = get_cluster_site(self.cluster)
        
        if not os.path.exists(self.config_file):
            logger.error("Params file " + self.params_file + " does not exist")
        
        # Set oar job id
        if self.options.oar_job_id:
            self.oar_job_id = self.options.oar_job_id
        else:
            self.oar_job_id = None

        # TODO: Provisional
        #(self.oar_job_id, self.frontend) = get_current_oar_jobs(frontends=["lyon"])[0]
        self.oar_job_id = get_current_oar_jobs()[0][0]

        # Main
        try:
            # Creation of the main iterator which is used for the first control loop.
            # You need have a method called define_parameters, that returns a list of parameter dicts
            self.define_parameters()
            
            job_is_dead = False
            # While they are combinations to treat
            while len(self.sweeper.get_remaining()) > 0:
                
                ## SETUP
                # If no job, we make a reservation and prepare the hosts for the experiments
                #if job_is_dead or self.oar_job_id is None:
                #    self.make_reservation()
                self.hosts = get_oar_job_nodes(self.oar_job_id, self.frontend)
                (deployed, undeployed) = self.deploy_nodes()
                if len(deployed) == 0:
                    break
                #else:
                #    self.hosts = get_oar_job_nodes(self.oar_job_id, self.frontend)
                if not self.hc:
                    self.hc = HadoopCluster(self.hosts)
                ## SETUP FINISHED

                # Getting the next combination (which requires a dataset deployment)
                comb = self.sweeper.get_next()
                self.comb = comb
                self.prepare_dataset(comb)
                self.xp(comb)
                                
                # subloop over the combinations that use the same dataset
                while True:
                    newcomb = self.sweeper.get_next(lambda r:
                            filter(self._uses_same_ds, r))
                    if newcomb:
                        try:
                            self.xp(newcomb)
                        except:
                            break
                    else:
                        break

                if get_oar_job_info(self.oar_job_id, self.frontend)['state'] == 'Error':
                    job_is_dead = True

        finally:
            if self.oar_job_id is not None:
                if not self.options.keep_alive:
                    pass
                    #logger.info('Deleting job')
                    #oardel([(self.oar_job_id, self.frontend)])
                else:
                    logger.info('Keeping job alive for debugging')
                    
            if self.hc:
                if self.hc.initialized:
                    self.hc.clean()
                    

    def _uses_same_ds(self, candidate_comb):
        """Determine if the candidate combination uses the same dataset as the
        current one.
        
        Args:
          candidate_comb (dict): The combination candidate to be selected as the
            new combination.
        """
        
        for var in self.ds_parameters.keys():
          if candidate_comb[var] != self.comb[var]:
            return False
        return True
      

    def define_parameters(self):
        """Create the iterator that contains the parameters to be explored."""
        
        config = ConfigParser.ConfigParser()
        config.readfp(open(self.config_file))
        
        ds_parameters_names = config.options("ds_parameters")
        xp_parameters_names = config.options("xp_parameters")              
        
        # DATASET PARAMETERS
        self.ds_parameters = {}
        ds_class_parameters = {}
        ds_classes = []
        for pn in ds_parameters_names:
            pv = config.get("ds_parameters", pn).split(",")
            if pn.startswith("ds.class."):
                ds_class_parameters[pn[len("ds.class."):]] = [v.strip() for v in pv]
            elif pn == "ds.class":
                ds_classes = [v.strip() for v in pv]
            else:
                self.ds_parameters[pn] = [v.strip() for v in pv]
                
        if not config.has_option("ds_parameters","ds.class.dest"):
            ds_class_parameters["dest"] = "${data_dir}"
        
        # Create ds configurations        
        self.ds_config = []
        for (idx, ds_class) in enumerate(ds_classes):
            this_ds_params = {}
            for pn, pv in ds_class_parameters.iteritems():
                if len(pv) == len(ds_classes):
                    if pv[idx]:
                        this_ds_params[pn] = pv[idx]                    
                elif len(pv) == 1:
                    this_ds_params[pn] = pv[0]
                else:
                    logger.error("Number of ds_class does not much number of " + pn)
                    # TODO: exeception                    

            self.ds_config.append((ds_class, this_ds_params))
        
        self.ds_parameters["ds.config"] = range(0,len(self.ds_config))
        
            
        # EXPERIMENT PARAMETERS
        self.xp_parameters = {}
        for pn in xp_parameters_names:
            pv = config.get("xp_parameters", pn).split(",")
            self.xp_parameters[pn] = [v.strip() for v in pv]           
            
        # GLOBAL
        self.parameters = { }
        self.parameters.update(self.ds_parameters)
        self.parameters.update(self.xp_parameters)
        
        
        # PRINT PARAMETERS
        print_ds_parameters = {}
        print_ds_parameters.update(self.ds_parameters)
        print_ds_parameters["ds.config"] = self.ds_config
        logger.info("Dataset parameters: " + str(print_ds_parameters))
        logger.info("Experiment parameters: " + str(self.xp_parameters))
        
        self.sweeper = ParamSweeper(os.path.join(self.result_dir, "sweeps"), 
                                    sweep(self.parameters))

        logger.info('Number of parameters combinations %s', len(self.sweeper.get_remaining()))

    def _import_class(self, name):
        """Dynamically load a class and return a reference to it.
        
        Args:
          name (str): the class name, including its package hierarchy.
          
        Returns:
          A reference to the class.
        """
        
        last_dot = name.rfind(".")
        package_name = name[:last_dot]
        class_name = name[last_dot + 1:]
        
        mod = __import__(package_name, fromlist=[class_name])
        return getattr(mod, class_name)

    def __get_ds_parameters(self, params):
        ds_params = {}
        for pn in self.ds_parameters:
            ds_params[pn] = params[pn]
        ds_params["ds.config"] = self.ds_config[params["ds.config"]]            
        return ds_params
        
    def __get_xp_parameters(self, params):
        xp_params = {}
        for pn in self.xp_parameters:
            xp_params[pn] = params[pn]
        return xp_params

    def _replace_macros(self, value):
        new_value = value
        for m in self.macros:
            new_value = new_value.replace(m, str(self.macros[m]))
            
        return new_value

    def make_reservation(self):
        """Perform a reservation of the required number of nodes."""

        logger.info('Performing reservation')
        starttime = int(time.time() + timedelta_to_seconds(datetime.timedelta(minutes=1)))
        endtime = int(starttime + timedelta_to_seconds(datetime.timedelta(days=3,
                                                                 minutes=1)))
        startdate, n_nodes = self._get_nodes(starttime, endtime)
        while not n_nodes:
            logger.info('Not enough nodes found between %s and %s, ' + \
                        'increasing time window',
                        format_date(starttime), format_date(endtime))
            starttime = endtime
            endtime = int(starttime + timedelta_to_seconds(datetime.timedelta(days=3,
                                                                minutes=1)))
            startdate, n_nodes = self._get_nodes(starttime, endtime)
            if starttime > int(time.time() + timedelta_to_seconds(
                                            datetime.timedelta(weeks=6))):
                logger.error('There are not enough nodes on %s for your ' + \
                             'experiments, abort ...', self.cluster)
                exit()
        jobs_specs = get_jobs_specs({self.cluster: n_nodes},
                                    name=self.__class__.__name__)
        sub = jobs_specs[0][0]
        sub.walltime = self.options.walltime
        sub.additional_options = '-t deploy'
        sub.reservation_date = startdate
        (self.oar_job_id, self.frontend) = oarsub(jobs_specs)[0]
        logger.info('Startdate: %s, n_nodes: %s', format_date(startdate),
                    str(n_nodes))
                    
    def _get_nodes(self, starttime, endtime):
        
        planning = get_planning(elements = [self.cluster],
                                starttime = starttime,
                                endtime = endtime,
                                out_of_chart = self.options.outofchart)
        slots = compute_slots(planning, self.options.walltime)
        startdate = slots[0][0]
        i_slot = 0
        n_nodes = slots[i_slot][2][self.cluster]
        while n_nodes < self.n_nodes:
            logger.debug(slots[i_slot])
            startdate = slots[i_slot][0]
            n_nodes = slots[i_slot][2][self.cluster]
            i_slot += 1
            if i_slot == len(slots) - 1:
                return False, False
        return startdate, self.n_nodes
    
    
    def deploy_nodes(self, min_deployed_hosts = 1, max_tries = 3):
        """Deploy nodes in the cluster. If the number of deployed nodes is less
        thatn the specified min, try again.
        
        Args:
          min_deployed_hosts (int, optional): minimum number of nodes to be
            deployed (defualt: 1).
          max_tries (int, optional): maximum number of tries to reach the
            minimum number of nodes (default: 3).
        """
        
        
        logger.info("Deploying " + str(len(self.hosts)) + " nodes")
        
        def correct_deployment(deployed, undeployed):
            return len(deployed) >= min_deployed_hosts
        
        (deployed, undeployed) = deploy(
            Deployment(self.hosts, 
            env_file = "/home/mliroz/deploys/hadoop6.env"),
            num_tries = max_tries,
            check_enough_func = correct_deployment,
            out=True
        )
            
        logger.info("%i deployed, %i undeployed" % (len(deployed), 
                                                    len(undeployed)))            
            
        if not correct_deployment(deployed, undeployed):
            logger.error("It was not possible to deploy min number of hosts")                             
                                        
        return (deployed, undeployed)
        
        
    def prepare_dataset(self, comb):
        """Prepare the dataset to be used in the next set of experiments.
        
        Args:
          comb (dict): The combination containing the dataset's parameters.        
        """
        
        logger.info("Prepare dataset with combination " + str(self.__get_ds_parameters(comb)))
        self.ds_id += 1
        self.__update_macros()
        
        # Initialize cluster and start
        self.hc.initialize()
        self.hc.start_and_wait()
        
        # Populate dataset
        self.deploy_ds(comb)
        
      
    def deploy_ds(self, comb):
        """Deploy the dataset corresponding to the given combination.
        
        Args:
          comb (dict): The combination containing the dataset's parameters.
        """
        
        # Create dataset
        ds_idx = comb["ds.config"]
        (ds_class_name, ds_params) = self.ds_config[ds_idx]
        ds_class = self._import_class(ds_class_name)
        self.ds = ds_class(ds_params)   
        
        
        # Deploy dataset
        def uncompress_function(file_name, host):
            #action = Remote("gzip -d " + file_name, [ host ])
            action = Remote("bzip2 -d " + file_name, [ host ])
            action.run()
        
            #return file_name[:-3] # Remove .gz (provisional)        
            return file_name[:-4] # Remove .bz2 (provisional)
        
        dest = self._replace_macros(ds_params["dest"])
                
        self.ds.deploy(self.hc, dest, int(comb["ds.size"]), uncompress_function)
         
      
    def xp(self, comb):
        """Perform the experiment corresponding to the given combination.
        
        Args:
          comb (dict): The combination wit the experiment's parameters.
        """
        
        comb_ok = False
        try:
            logger.info("Execute experiment with combination " + str(self.__get_xp_parameters(comb)))
            self.comb_id += 1
            self.__update_macros()

            # TODO - the experiment

            comb_ok = True
        finally:
            if comb_ok:
                self.sweeper.done(comb)
            else:
                self.sweeper.cancel(comb)
            logger.info('%s Remaining',len(self.sweeper.get_remaining()))        


if __name__ == "__main__":
    engine = HadoopEngine()    
    engine.start()