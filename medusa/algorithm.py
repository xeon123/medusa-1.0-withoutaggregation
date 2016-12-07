import json
import logging
import multiprocessing as mp
import time
from collections import Counter
from threading import Thread

import medusa
import os
import ranking
import simplecache
from medusa import settings
from medusa.decors import make_verbose
from medusa.execution import run_job
from medusa.filesystem import update_json_file
from medusa.hdfs import writeJobRunning, rmr
from medusa.jobsmanager import ExecutionJob
from medusa.jobsmanager import JobOutput
from medusa.medusasystem import get_running_clusters, my_apply_async_without_waiting
from medusa.namedtuples import set_execution_params
from medusa.scheduler.predictionranking import load_prediction
from medusa.settings import medusa_settings
from medusa.utility import majority


def run_execution(faults, jobs, reference_digests=None):
    """
    Executes the job from different ways (serial|processes)

    :param faults: (int) number of faults to tolerate
    :param jobs: (int, tuple) command line to execute

    :return:
    """
    logging.info("Execution mode: %s" % medusa_settings.execution_mode)

    _ranking = ranking.rank_clusters(get_running_clusters())
    simplecache.SimpleCache.set_pick_up_clusters([_rank.cluster for _rank in _ranking])

    if medusa_settings.execution_mode == "serial":
        group_data = run_execution_serial(
            faults, jobs)
    else:
        group_data = run_execution_threads(
            faults, jobs)

    return group_data


def run_execution_threads(faults, jobs):
    """
     Execute jobs in serial

    :param faults: (int) Number of faults to tolerate
    :param jobs: (list) list of Job structures
    :param reference_digests:
    :return:
    """

    group_jobs = []
    if not jobs:
        return group_jobs

    logging.info(" Running scheduling: %s" % medusa_settings.ranking_scheduler)

    # Setup a list of processes that we want to run
    output = mp.Queue()

    # Get process results from the output queue
    clusters_to_launch_job = pick_up_clusters(0)

    job_args = []
    for job in jobs:
        job_args.append(
            ExecutionJob(job.id, clusters_to_launch_job, job.command, job.output_path + '/part*', majority(faults)))

    logging.info("Running %s jobs..." % (len(job_args)))
    seffective_job_runtime = time.time()

    processes = []
    for execution_parameters in job_args:
        # Each thread executes a job in the respective clusters
        processes.append(Thread(target=run_job, args=(execution_parameters, output,)))

    # Run processes
    [p.start() for p in processes]
    [p.join() for p in processes]

    _output_list = output.get()
    logging.info("Run_job took %s" % str(time.time() - seffective_job_runtime))

    spart = time.time()
    _job_output = []
    for _output in _output_list:
        _job_output += _output

    job_output_list = [ parse_data(_joutput) for _joutput in _job_output ]

    logging.info("Parse_data took %s" % str(time.time() - spart))

    srverification = time.time()
    digests_matrix = []
    while True:
        successful, digests = run_verification(job_output_list)
        if not successful:
            if medusa_settings.relaunch_job_same_cluster:
                # relaunch job in the same cloud
                path_to_remove = os.path.dirname(execution_parameters.output_path)
                _relaunch_job_same_cluster(execution_parameters, path_to_remove)
            else:
                logging.debug("Re-launching job %s" % execution_parameters.command)
                execution_parameters = _relaunch_job_other_cluster(execution_parameters, jobs)

            _job_output = run_job(execution_parameters)
            for _output in _job_output:
                job_output_list.append(parse_data(_output[0]))
        else:
            digests_matrix.append(digests)
            break
    logging.info("Run_verification took %s" % str(time.time() - srverification))

    # save progress of the job
    filename = settings.get_temp_dir() + "/job_progress_log.json"
    step = 2
    update_json_file(filename, step)

    eeffective_job_runtime = time.time()
    span = str(eeffective_job_runtime - seffective_job_runtime)

    """ The total time that it took to execute all jobs """
    logging.info("Effective job run-time: %s" % span)

    return digests_matrix


def run_execution_serial(faults, jobs):
    """
     Execute jobs in serial

    :param faults: (int) Number of faults to tolerate
    :param jobs: (list) list of Job structures
    :return: list with the result of the selected digest. Ex: (True, {u'/aggregate-output/part-r-00000': u'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'})
    """

    group_jobs = []
    if not jobs:
        return group_jobs

    logging.info(" Running scheduling: %s" % medusa_settings.ranking_scheduler)

    clusters_to_launch_job = pick_up_clusters(0)

    job_args = []
    for job in jobs:
        job_args.append(
            ExecutionJob(job.id, clusters_to_launch_job, job.command, job.output_path + '/part*', majority(faults)))

        logging.debug("Clusters included %s" % clusters_to_launch_job)

    # if medusa_settings.relaunch_job_other_cluster and not aggregation:
    #     logging.warn("Please shut one cluster down... Execution will resume in 10 secs.")
    #     time.sleep(10)

    logging.info("Running %s jobs..." % (len(job_args)))
    seffective_job_runtime = time.time()

    digests_matrix = []
    for execution_parameters in job_args:
        _job_output_list = []
        while True:
            _job_output = run_job(execution_parameters)  # run job in the set of clusters
            for _output in _job_output:
                _job_output_list.append(parse_data(_output[0]))

            successful, digests = run_verification(_job_output_list)
            if not successful:
                if medusa_settings.relaunch_job_same_cluster:
                    # relaunch job in the same cloud
                    path_to_remove = os.path.dirname(execution_parameters.output_path)
                    _relaunch_job_same_cluster(execution_parameters, path_to_remove)
                else:
                    # if len(_failed_exec) > 0:
                    logging.debug("Re-launching job %s" % execution_parameters.command)
                    execution_parameters = _relaunch_job_other_cluster(execution_parameters, jobs)
            else:
                digests_matrix.append(digests)
                break

    # save progress of the job
    filename = settings.get_temp_dir() + "/job_progress_log.json"
    step = 2
    update_json_file(filename, step)

    eeffective_job_runtime = time.time()
    span = str(eeffective_job_runtime - seffective_job_runtime)

    """ The total time that it took to execute all jobs """
    logging.info("Effective job run-time: %s" % span)

    return digests_matrix


def parse_data(job_output):
    """
    Read the job output, write the result in the remote host, and return a job object.

    :param (string) result of job in string format
    :return Returns a Job object
    """
    job = JobOutput()
    job.loads(job_output)
    cluster = job.cluster
    logging.debug("Job finished at %s" % cluster)

    job_data = json.loads(job_output)
    job_prediction = load_prediction.apply_async(queue=cluster).get()
    job_prediction = json.loads(job_prediction)

    currentqueuecapacity = job_data["jobs"]["job"]["currentqueuecapacity"]
    hdfsbytesread = job_data["jobs"]["job"]["hdfsbytesread"]
    hdfsbyteswritten = job_data["jobs"]["job"]["hdfsbyteswritten"]
    maps = job_data["jobs"]["job"]["maps"]
    reduces = job_data["jobs"]["job"]["reduces"]
    total_time = job_data["jobs"]["job"]["totaltime"]
    mem_load = job_prediction["mem_load"]
    cpu_load = job_prediction["cpu_load"]

    step = medusa.simplecache.SimpleCache.get("step")

    if step == 0:
        _job = json.loads(medusa.simplecache.SimpleCache.get("job"))
        job_list = medusa.jobsmanager.Job(**_job)
        job_name = job_list.name
    else:
        _job = json.loads(medusa.simplecache.SimpleCache.get("aggregator"))
        aggregator_list = medusa.jobsmanager.Job(**_job)
        job_name = aggregator_list.name

    execution_params = set_execution_params(job_name, currentqueuecapacity, hdfsbytesread, hdfsbyteswritten,
                                            maps, reduces, mem_load, cpu_load, total_time)

    my_apply_async_without_waiting(writeJobRunning, queue=cluster, args=(json.dumps(execution_params._asdict()),))

    return job


def parse_digests(job_output):
    """
    return a list of digests
    """

    maj = majority(medusa_settings.faults)
    # list of dicts
    nset_digests = []
    for job in job_output:
        ndigests = {}
        for digest in job.digests:
            ndigests.update(digest)  # concat dicts

        nset_digests.append(ndigests)  # append it to a list

    keys = []
    for _d in job_output[0].digests:
        keys += _d.keys()

    digests_matrix = []
    for k in keys:
        temp_val = []
        for dset in nset_digests:
            temp_val.append(dset[k])

        v, k = Counter(temp_val).most_common(1)[0]

        if k >= maj:
            digests_matrix.append((True, v))
        else:
            return False, None

    return True, digests_matrix


@make_verbose
def run_verification(job_output):
    """
    Check the digests of the set of jobs that have run

    :param job_output (list) list of output of the jobs (json)

    :return (tuple) with the result of the validation (True|False) or the selected digest
    """
    result, selected_digest = parse_digests(job_output)

    if settings.medusa_settings.faults_left > 0:
        result = False
        settings.medusa_settings.faults_left -= 1

    if result:
        filename = settings.get_temp_dir() + "/job_progress_log.json"
        step = 3
        update_json_file(filename, step)

        return result, selected_digest

    return False, None


def run_verification_global(digests_matrix):
    """ check if the digests_matrix got all results of execution True

    :param digests_matrix (list) list with tuples with result of execution and the selected digests. [(True|False, "digest")]

    """
    for success in digests_matrix:
        if not success:
            return False

    return True


def _relaunch_job_same_cluster(execution_parameters, path):
    """ relaunch job in the same cloud.

    :param execution_parameters (ExecutionJob) parameters for job execution
    :param path (string) path to remove
    """
    clusters = execution_parameters.clusters
    for cluster in clusters:
        rmr.apply_async(
            queue=cluster, args=(path,)).get()

    logging.debug(">>>>> relaunching the job in the same cluster <<<<<<")
    logging.debug("Hosts: %s\tPath: %s" % (clusters, path))


def _relaunch_job_other_cluster(execution_parameters, jobs):
    """ deals with error that happens when a job did not return result in a specific interval.
    Therefore, it relaunches the job in another cluster.

     :param execution_parameters (ExecutionJob) parameters of execution
     :param jobs (list of Job) list of Job structure
    """

    for job in jobs:
        if job.id == execution_parameters.id:
            # new_clusters = _copy_and_aggregate_other_cluster(job, reference_digests, aggregation)
            new_clusters = list(set(pick_up_clusters(1)) - set(pick_up_clusters(0)))[0]
            execution_parameters.clusters = [new_clusters]

    return execution_parameters


def pick_up_clusters(step, force=False):
    """
    Get a set of running clusters that will be used to run the job

    :param step(int) step of execution
    :param force (boolean) force to execute the if clause
    :return list of clusters
    """

    if step == 0 or force:  # if it is the first time that we are getting a set of clusters
        n = majority(medusa_settings.faults)
        clusters = simplecache.SimpleCache.get_pick_up_clusters()[0:n]
    else:
        clusters = simplecache.SimpleCache.get_pick_up_clusters()

    return clusters
