from serialiser import *


class DemoBatchSystem(GenericBatchSystem):

    @staticmethod
    def get_mnemonic():
        return "demo"

    def __init__(self, scheduler_output_filenames, config):
        self.scheduler_output_filenames = scheduler_output_filenames
        self.config = config

    def get_worker_nodes(self):

        worker_nodes = [
            {
                "domainname": "dn1.foo.com",
                "state": "-", # This - here is way to cryptic :)
                "gpus": "", # This isn't used anywhere
                "np": 24,
                "core_job_map": {
                    "11": "j2",
                    "12": "j2"
                }
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
                "core_job_map": {2: "j2", 4: "j2"}
            }
        ]

        return worker_nodes

    def get_jobs_info(self):
        job_ids = "j1 j2 j3 j4 j5".split()
        job_states = "Q R C E W".split() # Can't see how those are represented in the screen
        usernames = "bill john".split()
        queue_names = "Urgent Foobar".split() # Those names don't seem to be used
        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        total_running_jobs = 1
        total_queued_jobs = 4

        qstatq_list = [
            #                     V- Why these should be strings (colorize)?
            {'run': 1, 'queued': '3', 'queue_name': 'Urgent', 'state': 'Q', 'lm': 0},
            {'run': 0, 'queued': '2', 'queue_name': 'Foobar', 'state': 'W', 'lm': 0}
        ]

        return total_running_jobs, total_queued_jobs, qstatq_list
