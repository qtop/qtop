from serialiser import *
import random
WORKER_NODES = 25
QUEUES = 3


class DemoBatchSystem(GenericBatchSystem):
    """
    This is an example implementation of how a batch system is "read" and what is expected of it
    by qtop in order to run.
    """

    @staticmethod
    def get_mnemonic():
        return "demo"

    def __init__(self, scheduler_output_filenames, config):
        """
        config corresponds to the QTOPCONF_YAML file distributed with qtop.
        Custom QTOPCONF files can be created and placed either
        in USERPATH/QTOPCONF_YAML or in SYSTEMCONFDIR/QTOPCONF_YAML
        """
        self.scheduler_output_filenames = scheduler_output_filenames
        self.config = config
        self.jobs = list()  # needed just for the demo to work

    def get_worker_nodes(self):
        """
        Possible node states are:
        "-": free
        "b": busy
        "d": down/offline
        and generally the first letter of the word describing the state is used.
        """

        worker_nodes = list()
        for i in range(WORKER_NODES):
            worker_node = dict()
            worker_node["domainname"] = "".join([
                random.choice(['dn', 'pc', 'wn']),
                str(random.randint(0,10)),
                random.choice(['foo.com', 'bar.com', 'baz.com'])])
            worker_node["state"] = random.choice(["-", "b", "d"])
            worker_node["gpus"] = random.choice([0, 2, 4, 8, 16, 24, 32])
            worker_node["np"] = random.choice([4, 8, 16, 24, 32])
            # pick a series of random core/random job nr pairs
            # worker_node["core_job_map"] = dict((random.choice(range(worker_node["np"])), "j" + str(random.randint(0, 500)))
            #                                    for i in range(random.randint(0, worker_node["np"])))
            worker_node["core_job_map"] = dict()
            for i in range(random.randint(0, worker_node["np"])):
                random_job_id = "j" + str(random.randint(0, 500))
                random_core = random.choice(range(worker_node["np"]))
                worker_node["core_job_map"][random_core] = random_job_id
                self.jobs.append(random_job_id)
            worker_nodes.append(worker_node)
        # worker_nodes = [
        #     {
        #         "domainname": "dn1.foo.com",
        #         "state": "-",
        #         "gpus": "",  # Values like this could be displayed by including them in the QTOPCONF file
        #         "np": 24,
        #         "core_job_map": {11: "j2", 12: "j2"}
        #     },
        #     {
        #         "domainname": "dn2.bar.com",
        #         "state": "-",
        #         "gpus": "",
        #         "np": 8,
        #         "core_job_map": {0: "j2", 1: "j2"}
        #     },
        #     {
        #         "domainname": "dn3.baz.com",
        #         "state": "-",
        #         "gpus": "",
        #         "np": 16,
        #         "core_job_map": {2: "j2", 4: "j1"}
        #     },
        #     {
        #         "domainname": "dn4.baz.com",
        #         "state": "b",
        #         "gpus": "",
        #         "np": 4,
        #         "core_job_map": {1: "j1", 3: "j5"}
        #     },
        #     {
        #         "domainname": "dn5.baz.com",
        #         "state": "-",
        #         "gpus": "",
        #         "np": 16,
        #         "core_job_map": {2: "j2", 4: "j2"}
        #     },
        #     {
        #         "domainname": "sf1.baz.com",
        #         "state": "-",
        #         "gpus": "",
        #         "np": 16,
        #         "core_job_map": {2: "j2", 4: "j4"}
        #     },
        #     {
        #         "domainname": "sf2.baz.com",
        #         "state": "d",
        #         "gpus": "",
        #         "np": 16,
        #         "core_job_map": {}
        #     }
        # ]

        return worker_nodes

    def get_jobs_info(self):
        """
        These 4 lists have to be of the same length (TODO: maybe make a tuple out of them or consider an alternative structure?)
        """
        job_ids = self.jobs
        # These are used in the upper and lower parts of qtop in the statistics part
        job_states = [random.choice("Q R C E W".split()) for _ in job_ids]
        usernames = [random.choice("bill john gus anthony thomas".split()) for _ in job_ids]
        queue_names = [random.choice("Urgent Foobar Urgent Foobar Priori".split()) for _ in job_ids]
        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        total_running_jobs = 110  # these are reported directly by qstat in PBS; if not, they are calculated.
        total_queued_jobs = 100

        qstatq_list = list()
        for i in range(QUEUES):
            qstatq = dict()
            qstatq['run'] = random.randint(0, 15)
            qstatq['queued'] = str(random.randint(0, 15))
            qstatq['queue_name'] = random.choice("Urgent Foobar Priori".split())
            qstatq['state'] = random.choice("Q R C E W".split())
            qstatq['lm'] = random.randint(0, 100)
            qstatq_list.append(qstatq)
        # qstatq_list = [
        #     #                     V- Why these should be strings (colorize)?
        #     {'run': 2, 'queued': '3', 'queue_name': 'Urgent', 'state': 'Q', 'lm': 0},
        #     {'run': 2, 'queued': '3', 'queue_name': 'Priori', 'state': 'Q', 'lm': 0},
        #     {'run': 1, 'queued': '2', 'queue_name': 'Foobar', 'state': 'W', 'lm': 0}
        # ]

        return total_running_jobs, total_queued_jobs, qstatq_list
