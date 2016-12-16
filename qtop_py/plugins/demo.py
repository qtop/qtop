import random
import itertools
import time
from qtop_py.serialiser import GenericBatchSystem
from collections import defaultdict

WORKER_NODES = 80
QUEUES = "urgent transfer batch".split()

AVG_JOB_DURATION = 5  # In units of refresh period (e.g. 2 seconds)
DESIRED_GRID_UTILIZATION = 0.75  # The grid won't be scheduled beydond this ratio

NODE_REPAIR_PROBABILITY = 0.03
NODE_FAILURE_PROBABILITY = 0.01

QUEUE_STATE_CHANGE_PROBABILITY = 0.05


class LittleGridSimulator(object):

    def __init__(self):
        # Initialize seeds from time
        e_time = int(time.time())

        rest, markov_iters = divmod(e_time, 100)
        _, cores_seed = divmod(rest, 1000) # Change board every 100 seconds

        random.seed(cores_seed)

        # -- Step 1. Setup random grid

        # Create random cores per worker node
        self.nps = []
        for i in range(WORKER_NODES):
            self.nps.append(random.choice([8, 16, 24, 32]))

        self.total_nps = sum(self.nps)

        # Create random node names...
        iter_names = itertools.cycle(['dn', 'pc', 'wn'])
        iter_domains = itertools.cycle(['foo.com', 'bar.com', 'baz.com'])

        self.domain_names = []
        for i in range(WORKER_NODES):
            domainname = "".join([
                next(iter_names),
                str(i),
                next(iter_domains)])
            self.domain_names.append(domainname)

        # Setup random node states
        self.node_state = []
        p_all = NODE_REPAIR_PROBABILITY + NODE_FAILURE_PROBABILITY
        for i in range(WORKER_NODES):

            if NODE_REPAIR_PROBABILITY < random.random() * p_all:
                state = random.choice(["-", "b"])
            else:
                state = random.choice(["d"])

            self.node_state.append(state)

        # Set all cores in all nodes to unscheduled state
        self.core_job_map = defaultdict(list)
        for node in range(WORKER_NODES):
            for core in range(self.nps[node]):
                self.core_job_map[node].append(None)

        # -- Step 2. Do a few iterations (at least 10) on the state of the cluster
        p_job_die = 1. / AVG_JOB_DURATION
        first = True
        for _ in (xrange(10 + markov_iters)):
            # Step 3.a Add a random number of jobs into each queue
            if first or self.get_total_queued() == 0:
                self.init_jobs()
                first = False

            # Step 3.b Clear all jobs which were scheduled to die
            for node in range(WORKER_NODES):
                for core, job_id in enumerate(self.core_job_map[node]):
                    if job_id:
                        if self.job_state.get(job_id, 'Not_existing!') != 'R':
                            self.core_job_map[node][core] = None


            # Step 3.c A few jobs will be scheduled to die...
            for node in range(WORKER_NODES):
                for core, job_id in enumerate(self.core_job_map[node]):
                    if job_id:
                        if random.random() < p_job_die:
                            self.job_state[job_id] = random.choice("C E W".split())


            # Step 3.d Make a few servers fail or get repaired
            for node in range(WORKER_NODES):
                if self.node_state[node] == "d":
                    if random.random() < NODE_REPAIR_PROBABILITY:
                        # We have a repair... ready to schedule
                        self.node_state[node] = random.choice(["-", "b"])
                else:
                    if random.random() < NODE_FAILURE_PROBABILITY:
                        # We have a failure. All jobs completed (if only!)
                        self.node_state[node] = "d"
                        for core, job_id in enumerate(self.core_job_map[node]):
                            if job_id:
                                self.core_job_map[node][core] = None
                                self.job_state[job_id] = random.choice("C E W".split())


            # Step 3.f Find available slots in the system...
            empty_slots = []
            utilized_slots = 0
            for node in range(WORKER_NODES):
                if self.node_state[node] != "d":
                    for core, job_id in enumerate(self.core_job_map[node]):
                        if job_id:
                            utilized_slots += 1
                        else:
                            empty_slots.append((node, core))

            total_available_capacity = utilized_slots + len(empty_slots)


            # Step 3.g Schedule up to the desired allocation...
            desired_allocated_slots = int(total_available_capacity * DESIRED_GRID_UTILIZATION)

            to_schedule = int(desired_allocated_slots - utilized_slots)

            if to_schedule > 0:

                # Pick the first `to_schedule` random slots
                random.shuffle(empty_slots)
                lucky_slots = empty_slots[:to_schedule]

                # and give them jobs from the queues
                for node, core in lucky_slots:
                    queue_names = self.queue_jobs.keys()
                    random.shuffle(queue_names)
                    for queue_name in queue_names:
                        if self.queue_jobs[queue_name]:
                            job_id = self.queue_jobs[queue_name].pop()
                            self.core_job_map[node][core] = job_id
                            self.job_state[job_id] = 'R'
                            break

            # Shuffle queue states as well, every now and then
            for queue_name in QUEUES:
                if random.random() < QUEUE_STATE_CHANGE_PROBABILITY:
                    self.queue_state[queue_name] = random.choice("Q R C E W".split())

    def get_total_queued(self):
        total_queued_jobs = 0
        for queue_name in QUEUES:
            total_queued_jobs += len(self.queue_jobs[queue_name])
        return total_queued_jobs

    def init_jobs(self):
        jobcnt = 0
        self.queue_jobs = defaultdict(list)
        self.job_meta = {}
        self.job_state = {}
        self.queue_state = {}
        for queue_name in QUEUES:
            # Add a random number of jobs into each queue
            for jobs in xrange(random.randint(100, 3000)):
                job_id = "j%d" % jobcnt
                username = random.choice("alice023 cms347 cms125 lhcbplt01 cms360 Atlassm".split())
                self.queue_jobs[queue_name].append(job_id)
                self.job_state[job_id] = 'Q'  # Initialy everything queued
                self.job_meta[job_id] = (queue_name, username)
                jobcnt += 1
            self.queue_state[queue_name] = random.choice("Q R C E W".split())


class DemoBatchSystem(GenericBatchSystem):
    """
    This is an example implementation of how a batch system is "read" and what is expected of it
    by qtop in order to run.
    """

    @staticmethod
    def get_mnemonic():
        return "demo"

    def __init__(self, scheduler_output_filenames, config, options):
        """
        config corresponds to the QTOPCONF_YAML file distributed with qtop.
        Custom QTOPCONF files can be created and placed either
        in USERPATH/QTOPCONF_YAML or in SYSTEMCONFDIR/QTOPCONF_YAML
        """
        self.scheduler_output_filenames = scheduler_output_filenames
        self.config = config
        self.sim = LittleGridSimulator()


    def get_worker_nodes(self, job_ids, job_queues, options):
        """
        Possible node states are:
        "-": free
        "b": busy
        "d": down/offline
        and generally the first letter of the word describing the state is used.
        """

        worker_nodes = []

        for node in range(WORKER_NODES):

            # Create a map core=>jobid
            core_job_map = {}
            for core, job_id in enumerate(self.sim.core_job_map[node]):
                if job_id:
                    core_job_map[core] = job_id

            worker_node = {
                "domainname": self.sim.domain_names[node],
                "state": self.sim.node_state[node],
                "np": self.sim.nps[node],
                "core_job_map": core_job_map
            }

            worker_nodes.append(worker_node)
        worker_nodes = self.ensure_worker_nodes_have_qnames(worker_nodes, job_ids, job_queues)
        return worker_nodes

    def get_jobs_info(self):
        """
        These 4 lists have to be of the same length (TODO: maybe make a tuple out of them or consider an alternative structure?)
        """
        job_ids = []
        usernames = []
        job_states = []
        queue_names = []
        for node in range(WORKER_NODES):
            for core, job_id in enumerate(self.sim.core_job_map[node]):
                if job_id:
                    job_ids.append(job_id)
                    
                    queue_name, username = self.sim.job_meta[job_id]
                    usernames.append(username)
                    queue_names.append(queue_name)

                    job_states.append(self.sim.job_state[job_id])

        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        total_running_jobs = 0  # these are reported directly by qstat in PBS; if not, they are calculated.
        run_for_queue = defaultdict(int)
        for node in range(WORKER_NODES):
            for core, job_id in enumerate(self.sim.core_job_map[node]):
                if job_id:
                    total_running_jobs += 1
                    queue_name, _ = self.sim.job_meta[job_id]
                    run_for_queue[queue_name] += 1

        total_queued_jobs = self.sim.get_total_queued()

        qstatq_list = list()
        
        for queue_name in QUEUES:
            qstatq = {
                'queue_name': queue_name,
                'run': run_for_queue[queue_name],
                'queued': str(len(self.sim.queue_jobs[queue_name])),
                'state': self.sim.queue_state[queue_name],
                'lm': random.randint(0, 100),
            }
            qstatq_list.append(qstatq)

        return total_running_jobs, total_queued_jobs, qstatq_list
