from serialiser import *


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

    def get_worker_nodes(self):
        """
        Possible node states are:
        "-": free
        "b": busy
        "d": down/offline
        and generally the first letter of the word describing the state is used.
        """
        worker_nodes = [
            {
                "domainname": "dn1.foo.com",
                "state": "-",
                "gpus": "",  # Values like this could be displayed by including them in the QTOPCONF file
                "np": 24,
                "core_job_map": {11: "j2", 12: "j2"}
            },
            {
                "domainname": "dn2.bar.com",
                "state": "-",
                "gpus": "",
                "np": 8,
                "core_job_map": {0: "j2", 1: "j2"}
            },
            {
                "domainname": "dn3.baz.com",
                "state": "-",
                "gpus": "",
                "np": 16,
                "core_job_map": {2: "j2", 4: "j1"}
            },
            {
                "domainname": "dn4.baz.com",
                "state": "b",
                "gpus": "",
                "np": 4,
                "core_job_map": {1: "j1", 3: "j5"}
            },
            {
                "domainname": "dn5.baz.com",
                "state": "-",
                "gpus": "",
                "np": 16,
                "core_job_map": {2: "j2", 4: "j2"}
            },
            {
                "domainname": "sf1.baz.com",
                "state": "-",
                "gpus": "",
                "np": 16,
                "core_job_map": {2: "j2", 4: "j4"}
            },
            {
                "domainname": "sf2.baz.com",
                "state": "d",
                "gpus": "",
                "np": 16,
                "core_job_map": {}
            }
        ]

        return worker_nodes

    def get_jobs_info(self):
        """
        These 4 lists have to be of the same length (TODO: maybe make a tuple out of them or consider an alternative structure?)
        """
        job_ids = "j1 j2 j3 j4 j5".split()
        job_states = "Q R C E W".split()  # These are used in the upper and lower parts of qtop, in the statistics part
        usernames = "bill john gus anthony thomas".split()
        queue_names = "Urgent Foobar Urgent Foobar Priori".split()
        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        total_running_jobs = 11  # these are reported by qstat in PBS. If not they are calculated.
        total_queued_jobs = 1

        qstatq_list = [
            #                     V- Why these should be strings (colorize)?
            {'run': 2, 'queued': '3', 'queue_name': 'Urgent', 'state': 'Q', 'lm': 0},
            {'run': 2, 'queued': '3', 'queue_name': 'Priori', 'state': 'Q', 'lm': 0},
            {'run': 1, 'queued': '2', 'queue_name': 'Foobar', 'state': 'W', 'lm': 0}
        ]

        return total_running_jobs, total_queued_jobs, qstatq_list
