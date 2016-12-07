import json
import logging
import math
import multiprocessing as mp
from threading import Thread

import jobsmanager

import medusa
import numpy as np
import simplecache
from celery import task
from medusa import settings
from medusa.filesystem import write_data, read_data
from medusa.mapreduce import get_queue_info, read_remote_job_data
from medusa.namedtuples import set_job_params, set_penalization_params, set_job_rank
from medusa.numpylinearregression import estimate_job_execution, calculate_linear_regression
from medusa.settings import getQueueJSON, medusa_settings
from medusa.utility import get_cpu_load, get_mem_load

"""
 Job Queue metrics
"""


def get_prediction_metrics(clusters, pinput, input_size):
    """
    rank jobs using linear regression

    :param input_size:
    :param clusters (list) list of clusters
    :param pinput (list) list of input paths

    :return Get a list of namedtuple JobRank
    """

    """ get metrics related to job """
    output = mp.Queue()
    rank_list = []
    if medusa_settings.execution_mode == "serial":
        for cluster in clusters:
            _job_rank = get_prediction_metrics_on_job(cluster, input_size)
            _job_rank = set_job_rank(_job_rank[0], _job_rank[1])
            rank_list.append(_job_rank)
    else:
        processes = []
        for cluster in clusters:
            # Each thread executes a job in the respective clusters
            processes.append(Thread(target=get_prediction_metrics_on_job, args=(cluster, input_size, output,)))

        # Run processes
        [p.start() for p in processes]
        [p.join() for p in processes]


        for _ in processes:
            _job_rank = output.get()
            rank_list.append(set_job_rank(_job_rank[0], _job_rank[1]))

    logging.info("Ranking: %s - %s" % (pinput, rank_list))

    return rank_list


def get_prediction_metrics_on_job(cluster, input_size, queue=None):
    """

    :param cluster: (string) cluster of the job
    :param input_size:
    :para queue (string)
    :return: (float) time to execute
    """
    task1 = medusa.medusasystem.my_apply_async_without_waiting(read_remote_job_data, queue=cluster)
    task2 = medusa.medusasystem.my_apply_async_without_waiting(get_queue_info, queue=cluster)

    queue_data = task2.get()
    queue_json = json.loads(getQueueJSON(queue_data))

    data_file = task1.get()
    current_capacity = float(queue_json["currentqueuecapacity"])
    data = json.loads(data_file)

    """
      logline = "%s:%s:%s:%s:%s:%s:%s:%s" %(data['cluster'],
                                   data['currentqueuecapacity'],
                                   data['hdfsbytesread'],
                                   data['hdfsbyteswritten'],
                                   data['maps'],
                                   data['reduces'],
                                   data['cpu'],
                                   data['mem']
                                   data['totaltime'])
    """
    # put data in a matrix
    params_matrix = []
    params_matrix2 = []
    for _line in data["data"][:100]:
        params_matrix.append([
            float(_line["currentqueuecapacity"]),
            float(_line["hdfsbytesread"]),
            float(_line["hdfsbyteswritten"]),
            float(_line["maps"]),
            float(_line["reduces"]),
            float(_line["cpu"]),
            float(_line["mem"])])

        params_matrix2.append([
            float(_line["hdfsbytesread"]),
            float(_line["hdfsbyteswritten"]),
            _line["job_name"]])

    time_matrix = [float(line["time"]) for line in data["data"]]

    xx = np.array(params_matrix)
    yy = np.array(time_matrix)

    # coeffs for the params:
    # currentqueuecapacity:hdfsbytesread:maps:CPU:MEM
    model = calculate_linear_regression(yy, xx)
    coeffs = model.params
    cpu_load = get_cpu_load.apply_async(queue=cluster).get()
    mem_load = get_mem_load.apply_async(queue=cluster).get()

    step = simplecache.SimpleCache.get("step")

    if step == 0:
        job_list = json.loads(simplecache.SimpleCache.get("job"))
        job = jobsmanager.Job(**job_list)
        job_name = job.name


    output_size = _filter_output_data(input_size, job_name, params_matrix2)

    maps = int(math.ceil((input_size * 1.0) / medusa_settings.blocksize))

    job_params = set_job_params(job_name, current_capacity, input_size, output_size, maps, cpu_load, mem_load)
    time = estimate_job_execution(coeffs, job_params)

    job_params = set_job_params(job_name, current_capacity, input_size, output_size, maps, cpu_load, mem_load, time)
    medusa.medusasystem.my_apply_async_without_waiting(medusa.scheduler.predictionranking.save_prediction, queue=cluster, args=(json.dumps(job_params._asdict()),))

    if queue is not None:
        queue.put((cluster, time))
        return

    return cluster, time


def _filter_output_data(input_size, job_name, params_matrix):
    """
    Predict the output data of the job by doing an arithmetic mean the the previous results that got simila input data
    :param input_size: size of the input data
    :param job_name: name of the job
    :param params_matrix: predicted output data
    :return:
    """
    limit = input_size * 0.1
    min_input_size = input_size - limit
    max_input_size = input_size + limit

    sum_value = 0
    iter = 0
    predicted_output_data = 0
    for params in params_matrix:
        if min_input_size < params[0] < max_input_size \
                and job_name == params[-1]:
            sum_value += params[1]
            iter += 1

    if iter > 0:
        predicted_output_data = sum_value / iter

    return predicted_output_data


@task(name='medusa.scheduler.predictionranking.save_prediction')
def save_prediction(job_params):
    """
    Save prediction values of the job into a file
    :param job_params (string) data to be saved
    """

    prediction_file = "%s/prediction.json" % settings.get_temp_dir()
    write_data(prediction_file, job_params)


@task(name='medusa.scheduler.predictionranking.load_prediction')
def load_prediction():
    """
    Load prediction values of the job into a file
    :return:
    """

    prediction_file = "%s/prediction.json" % settings.get_temp_dir()
    data = read_data(prediction_file)[0]

    return data


@task(name='medusa.scheduler.predictionranking.reset_prediction')
def reset_prediction():
    """
    Reset prediction values of the job into a file
    """
    save_prediction(json.dumps(set_job_params(0, 0, 0, 0, 0, 0, 0)._asdict()))


@task(name='medusa.scheduler.predictionranking.save_penalization')
def save_penalization(penalization_values):
    """ Save penalization values """
    prediction_file = "%s/penalization.json" % settings.get_temp_dir()

    with open(prediction_file, 'w') as the_file:
        the_file.write(penalization_values)


@task(name='medusa.scheduler.predictionranking.reset_penalization')
def reset_penalization():
    """
    Reset the prediction file
    """
    save_penalization(json.dumps(set_penalization_params(0, 0, 0)._asdict()))
