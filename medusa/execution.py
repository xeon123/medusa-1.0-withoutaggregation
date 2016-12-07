import logging
import multiprocessing as mp
import shlex
import subprocess
import time
import traceback

import medusa
from celery import group, task
from medusa.decors import make_verbose
from medusa.hdfs import ls
from medusa.settings import getJobJSON, medusa_settings
from threading import Thread


@make_verbose
def run_job(execution_param, queue=None):
    """
    execute a job.

    :param queue:
    :param execution_param (ExecutionJob) object used to prepare the execution of the job.
    """

    clusters = execution_param.clusters
    how_many_runs = execution_param.how_many_runs
    command = execution_param.command
    output_path = execution_param.output_path

    # Setup a list of processes that we want to run
    output = mp.Queue()

    executors = []
    try:
        for idx, cluster in enumerate(clusters):
            if idx < how_many_runs:

                # execute the job
                logging.info("Executing job at %s " % cluster)
                g1 = group(executeCommand.s(command, 1, ).set(queue=cluster),
                           medusa.mapreduce.get_queue_info.s().set(queue=cluster))
                executors.append(medusa.jobsmanager.JobExecution(g1(), cluster, time.time()))
    except:
        logging.error(str(traceback.format_exc()))

    failed_exec = []
    processes = []
    for executor in executors:
        try:
            # Each thread executes a job in the respective clusters
            processes.append(Thread(target=get_job_output_generate_digest, args=(executor, output_path, output,)))
        except Exception:
            failed_exec.append(execution_param)

    [p.start() for p in processes]
    [p.join() for p in processes]

    json_results = [output.get() for _ in processes]

    if queue is not None:
        queue.put(json_results)
        return

    return json_results


def get_job_output_generate_digest(executor, output_path, queue):
    json_results = []
    _exec = executor.get_execution()
    cluster = executor.get_cluster()

    try:
        """ waiting for a task to finish """
        medusa.medusasystem.waiting(cluster, _exec)
        _output = _exec.get()
    except Exception:
        raise Exception

    job_output, queue_info = _output

    makespan = time.time() - executor.get_start_time()
    logging.info("Job executed in %s seconds", makespan)
    dstart = time.time()

    files = medusa.medusasystem.my_apply_async_with_waiting(ls, queue=cluster, args=(output_path,))

    tasks = []
    for _file in files:
        tasks.append(
            medusa.medusasystem.generate_one_digest.s(_file, medusa_settings.digest_command).set(queue=cluster))
    g1 = group(tasks)()

    while g1.waiting():
        time.sleep(2)

    digests = g1.get()

    logging.info("Digests generated in %s seconds", time.time() - dstart)

    if not "FileAlreadyExistsException" in job_output:
        json_out = getJobJSON(job_output, cluster, queue_info, makespan, digests)
        json_out = json_out.replace("\n", "").replace("\'", "\"")

        logging.debug("Got result from %s" % cluster)
        json_results.append(json_out)

        # if medusa_settings.ranking_scheduler == "prediction":
        #     f = my_apply_async(load_prediction, queue=cluster)
        #
        #     value = f.get()
        #     prediction_value = json.loads(value)["total_time"]
        # error = makespan - prediction_value
        # penalization_params = set_penalization_params(makespan, prediction_value, error)

        # logging.info("Job executed in %s seconds; predicted: %s seconds;", makespan, prediction_value)

        # my_apply_async(save_penalization, queue=cluster, args=(json.dumps(penalization_params._asdict()),)).get()
    else:
        raise Exception

    if queue is not None:
        queue.put(json_results)
        return

    return json_results


@task(name='medusa.execution.executeCommand')
@make_verbose
def executeCommand(command, stdout=0):
    """ Execute locally the command """

    logging.debug("Executing: %s" % command)
    process = subprocess.Popen(
        shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = process.communicate()
    output = out[stdout]

    logging.debug("Result of command %s: %s" % (command, output))

    # If we were capturing, this will be a string; otherwise it will be None.
    return output


def local_execute_command(command, stdout=0):
    """ Execute locally the command """

    logging.debug("Executing: %s" % command)
    process = subprocess.Popen(
        shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = process.communicate()
    output = out[stdout]

    logging.debug("Result of command %s: %s" % (command, output))

    # If we were capturing, this will be a string; otherwise it will be None.
    return output
