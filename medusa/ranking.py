import logging
import time

import medusa
import simplecache
from celery import group
from medusa import hdfs
from medusa.decors import make_verbose
from medusa.settings import medusa_settings

"""
 rank clusters according several metrics
"""


@make_verbose
def rank_clusters(clusters):
    """
        Rank the clusters

        :param clusters:

        :return The rank is saved in a tuple with the form (included set, excluded set) and returned
        Included and excluded set are ("cluster":points)
        e.g. ([('adminuser-VirtualBox-073n', 28.6)], [('adminuser-VirtualBox-074n', 24.7)])
    """

    rstart = time.time()
    rank_name = medusa_settings.ranking_scheduler

    rank = ()
    input_path = simplecache.SimpleCache.get_input_path()
    if rank_name == "prediction":
        processes = []
        for path in input_path:
            processes.append(medusa.medusasystem.my_apply_async(hdfs.ls, queue=clusters[0], args=(path,)))

        input_files = []
        for _p in processes:
            input_files += _p.get()

        tasks = []
        for path in input_files:
            tasks.append(hdfs.get_total_size.s(path).set(queue=clusters[0]))
        g1 = group(tasks)()
        input_size = sum(map(int, g1.get()))

        rank = medusa.scheduler.predictionranking.get_prediction_metrics(clusters, input_path, input_size)
    elif rank_name == "random":
        rank = medusa.scheduler.randomranking.get_random_metrics(clusters, input_path)

    logging.info("Host ranking list: %s, Duration %s " % (str(rank), str(time.time() - rstart)))

    return rank
