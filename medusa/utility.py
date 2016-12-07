import logging

import psutil
from celery import task
from medusa.settings import medusa_settings

"""
It takes information about the CPU
"""

@task(name='medusa.system.get_cpu_load')
def get_cpu_load():
    """
    Get CPU load

    :return: (float) percentage of cpu
    """
    return psutil.cpu_percent()


@task(name='medusa.system.get_mem_load')
def get_mem_load():
    """
    Get Memory usage load

    :return: (float) percentage of cpu
    """
    return psutil.virtual_memory().percent

@task(name='medusa.utility.getNamenodeAddress')
def getNamenodeAddress():
    logging.debug(
        "%s - %s - %s" % (medusa_settings.httpfs_used, medusa_settings.namenode_address, medusa_settings.httpfs_port))

    port = medusa_settings.httpfs_port if medusa_settings.httpfs_used else medusa_settings.hdfs_port
    address = "%s:%s" % (medusa_settings.namenode_address, port)

    return address


def majority(faults):
    """
    get majority value
    n = 2f + 1
    f = (n-1)/2
    n-f=majority
    """
    n = 2 * faults + 1

    return n - faults
