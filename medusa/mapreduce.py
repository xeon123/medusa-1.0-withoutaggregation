import medusa
from celery import task
from hadoopy._hdfs import _checked_hadoop_fs_command
from medusa import settings
from medusa.execution import local_execute_command
from medusa.local import lcat

"""
 creates the Map reduce commands that it is used
"""


def status(jobid):
    """ -status <job-id>    Prints the map and reduce completion percentage and all job counters."""
    return "%s job -status %s" % (settings.get_hadoop_bin(), jobid)


def getTotalJobsRunning():
    """ get the the number of jobs running """
    return "%s/jobslist.sh " % settings.get_medusa_home() + "/scripts"


def getQueueCapacity():
    """ get the the number of jobs running """
    TEMP = "/root/Programs/medusa_hadoop/scripts/"
    return "%s/queuecapacity.sh " % TEMP


def getPredictionCapacity():
    """ get the the number of jobs running """
    return "%s/predictioncapacity.sh %s" % (
        settings.get_medusa_home() + "/scripts", settings.get_medusa_home() + "/scripts")


def getJobsHistory():
    """ get the job history
    <data>
        <job>
            <name>job_1371099936192_0061</name>
            <status>SUCCEEDED</status>
            <span>17512</span>
            <nrmaps>Number of maps: 2</nrmaps>
            <nrreduces>Number of reduces: 1</nrreduces>
            <mapcompletion>map() completion: 1.0</mapcompletion>
            <redcompletion>reduce() completion: 1.0</redcompletion>
            <hdfs>Number of bytes read=47804753</hdfs>
            <hdfs>Number of bytes written=13157708</hdfs>
        </job>
    </data>
     """
    return "%s/jobshistory.sh " % settings.get_medusa_home() + "/scripts"


@task(name='medusa.mapreduce.get_queue_info')
def get_queue_info():
    """ get info of the queue """
    cmd = "mapred queue -list"
    rcode, stdout, stderr = _checked_hadoop_fs_command(cmd)
    return stdout

@task(name='medusa.mapreduce.read_remote_job_data')
def read_remote_job_data():
    """
    Read job data

    :return: output of the command
    """
    command = lcat(settings.get_temp_dir() + "/" + "job_log.json")
    output = medusa.execution.local_execute_command(command)
    return output
