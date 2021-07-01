from common.utils import check_command_output
import json
import logging


def _run_bjobs(full_format=False, hostname_filter=None, job_state_filter=None):
#   pending_filter = '-p' if job_state_filter else ''
    if job_state_filter == 'p':
        cmd = 'bjobs -p -o "JOBID QUEUE STAT JOB_NAME" -json'
    else:    
        cmd = 'bjobs -o "JOBID QUEUE STAT JOB_NAME" -json'
        
    return check_command_output(cmd, raise_on_error=False)

def _run_bhosts(full_format=True, hostname_filter=None, job_state_filter=None):
    cmd = 'bhosts -o "HOST_NAME STATUS MAX NJOBS RUN SSUSP USUSP RSV" -json'
    return check_command_output(cmd, raise_on_error=False)

def get_compute_nodes_info(hostname_filter=None, job_state_filter=None):
    output = _run_bhosts(full_format=True, hostname_filter=hostname_filter, job_state_filter=job_state_filter)
    if not output:
        return {}
    return json.loads(output)

def get_jobs_info(hostname_filter=None, job_state_filter=None):
    output = _run_bjobs(full_format=False, hostname_filter=hostname_filter, job_state_filter=job_state_filter)
    if not output:
        return []
      
    return json.loads(output)


def get_pending_jobs_info(max_slots_filter=None, skip_if_state=None, log_pending_jobs=True):
    """
    Retrieve the list of pending jobs.

    :param max_slots_filter: discard jobs that require a number of slots bigger than the given value
    :param skip_if_state: discard jobs that are in the given state
    :param log_pending_jobs: log the actual list of pending jobs (rather than just a count)
    :return: the list of filtered pending jos.
    """
    pending_jobs = get_jobs_info(job_state_filter="p")
    logging.info("Retrieved {0} pending jobs".format(len(pending_jobs)))
    return pending_jobs