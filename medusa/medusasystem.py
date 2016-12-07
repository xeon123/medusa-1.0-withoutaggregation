import hashlib
import logging
import subprocess
import time

import hadoopy
import ranking
import simplecache
import simplejson as json
from celery import task
from celery.exceptions import TimeoutError
from celery.result import AsyncResult
from medusa import settings
from medusa.decors import make_verbose
from medusa.namedtuples import get_reference_digest
from medusa.pingdaemon import clusterWorking
from medusa.settings import medusa_settings
from medusa.simplecache import MemoizeCalls


@make_verbose
def execute_and_get_digests(run_command, gen_digests_command):
    # return HDFS_BIN + " -ls -R " + path
    return (
        "%s/execute.sh \"%s\" \"%s\"" % (settings.get_medusa_home() + "/scripts",
                                         run_command, gen_digests_command)
    )


@task(name='medusa.medusasystem.hello')
@make_verbose
def hello(hostname):
    return "Hello %s" % hostname


@task(name='medusa.medusasystem.error_handler')
def error_handler(uuid):
    """
    Function that handle errors
    """

    result = AsyncResult(uuid)
    exc = result.get(propagate=False)
    logging.error(('Task %r raised exception: %r\n' % (
        exc, result.traceback)))


@task(name='medusa.medusasystem.generate_digests')
def generate_digests(pinput, digest_command):
    """
    generate digests from the input
    :param pinput (string) the input path
    :param digest_command (string) tells if should execute the -text or -cat command
    :return a dict with pairs <file, digest>
    """
    digests = {}

    try:
        files = hadoopy.ls(pinput)
    except IOError:
        return digests

    for afile in files:
        if digest_command == "-cat":
            stdout, stderr = subprocess.Popen([get_hadoop_path() + "/bin/hadoop", "fs", "-cat", afile],
                                              stdout=subprocess.PIPE).communicate()
        elif digest_command == "-text":
            stdout, stderr = subprocess.Popen([get_hadoop_path() + "/bin/hadoop", "fs", "-text", afile],
                                              stdout=subprocess.PIPE).communicate()

        m = hashlib.sha256()
        m.update(stdout)

        digests.update({afile: m.hexdigest()})

    return digests


@task(name='medusa.medusasystem.generate_one_digest')
def generate_one_digest(afile, digest_command):
    """
    generate digests from the input
    :param afile (string) the file name
    :param digest_command (string) tells if should execute the -text or -cat command
    :return a dict with pairs <file, digest>
    """

    if digest_command == "-cat":
        stdout, stderr = subprocess.Popen([get_hadoop_path() + "/bin/hadoop", "fs", "-cat", afile],
                                          stdout=subprocess.PIPE).communicate()
    elif digest_command == "-text":
        stdout, stderr = subprocess.Popen([get_hadoop_path() + "/bin/hadoop", "fs", "-text", afile],
                                          stdout=subprocess.PIPE).communicate()

    m = hashlib.sha256()
    m.update(stdout)

    return {afile: m.hexdigest()}


def readFileDigests(paths):
    """
    get digests from a set of paths
    :param paths list of input paths
    """
    # digest_values = {} if not aggregation else []
    digest_values = []
    clusters = get_running_clusters()
    func_cache = []
    for q in clusters:
        for path in paths:
            logging.debug("Path %s" %path)
            f = my_apply_async(
                generate_digests, queue=q, args=(path, medusa_settings.digest_command,))
            func_cache.append(MemoizeCalls(path, q, f))

    for memcache in func_cache:
        logging.debug("Cache: %s, %s" % (memcache.path, memcache.queue))
        digests = memcache.func.get()
        if len(digests) > 0:
            q = memcache.queue

            digest_values.append(get_reference_digest(q, digests))

    return digest_values


# demo functions
def func1():
    for i in range(1000):
        pass


@task(name='medusa.medusasystem.func2')
@make_verbose
def func2():
    for i in range(1000):
        func1()


@task(name='medusa.medusasystem.delay')
def delay():
    logging.info("Going to sleep...")
    time.sleep(10)
    logging.info("Waking up...")
    return "up"


@task(name='medusa.medusasystem.ping')
def ping():
    return "up"


# util functions
@task(name='medusa.medusasystem.get_hadoop_path')
def get_hadoop_path():
    return settings.get_hadoop_home()


def my_apply_async(f, **kwargs):
    return f.apply_async(**kwargs)


def my_apply_async_with_waiting(f, **kwargs):
    """ wait for a task to finish """
    f = my_apply_async(f, **kwargs)
    waiting(kwargs['queue'], f)
    return f.get()


def my_apply_async_without_waiting(f, **kwargs):
    """ wait for a task to finish """
    f = my_apply_async(f, **kwargs)
    return f


def waiting(cluster, f):
    """ wait for a task to finish """
    error_counter = 0
    while not f.ready():
        try:
            f1 = my_apply_async(ping, queue=cluster)
            f1.wait(timeout=None, interval=5)
        except TimeoutError:
            error_counter += 1
            if error_counter < 3:
                continue
            else:
                logging.warn("WARNING: Cluster %s is down" % cluster)
                set_running_clusters()
                raise Exception("Job execution failed")

    return f


def my_get(f):
    try:
        return f.get()
    except IOError, e:
        raise IOError('Function got an IO error', e)


def set_running_clusters():
    """
    save running clusters in a file
    """
    active = []
    for q in medusa_settings.clusters:
        if _isClusterWorking(q) is not None:
            active.append(q)

    # line = getActiveClustersJSON(active)
    filename = settings.get_medusa_home() + "/temp/clusters.json"
    with open(filename, 'w') as out_file:
        out_file.write(json.dumps(active))


def get_running_clusters():
    """
    get a list of running clusters
    """

    filename = settings.get_medusa_home() + "/temp/clusters.json"
    with open(filename, 'r') as in_file:
        clusters = json.load(in_file)

    return clusters


def _isClusterWorking(q, timeout=5):
    # f = my_apply_async(clusterWorking, queue=q)
    f = clusterWorking.apply_async(queue=q)
    try:
        result = f.get(timeout)
    except TimeoutError:
        logging.error("%s is down" % q)
        return None

    return result


def rank_clusters():
    """
    Rank clusters
    :return:
    """
    running_clusters = get_running_clusters()
    simplecache.SimpleCache.set_pick_up_clusters(ranking.rank_clusters(running_clusters))
