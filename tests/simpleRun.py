import json
import logging
import time
from multiprocessing.pool import ThreadPool

import medusa
from medusa import xmlparser
from medusa.algorithm import run_execution, run_verification_global
from medusa.medusasystem import readFileDigests, set_running_clusters


# Runs an example that it is defined in the wordcount
def test_run():
    # read wordcount xml
    # cluster1: job1 --> aggregation: job3
    # cluster2: job2 -----^

    format= "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(format=format, level=logging.DEBUG)

    path = "/root/Programs/medusa-1.0/submit/job.xml"
    from pudb import set_trace; set_trace()
    logger = logging.getLogger(__name__)

    faults_tolerate = 1
    job_list = xmlparser.parser(path, faults_tolerate, "job")

    medusa.simplecache.SimpleCache.save("job", json.dumps(job_list[0].__dict__))
    medusa.simplecache.SimpleCache.set_input_path([job_list[0].input_path])

    sequence = [job_list]

    pool = ThreadPool(processes=4)
    step = 0
    while step < len(sequence):
        jobs = sequence[step]
        medusa.simplecache.SimpleCache.save("step", step)

        if len(jobs) == 0:
            step += 1
            continue

        logger.info("Step %s starting" % step)
        if step == 0:
            logger.info("Checking clusters that are running...")
            set_running_clusters()

        # prepare environment for the test
        logger.info("Generating reference digests...")
        ss = time.time()

        reference_digests = []
        plist = [pool.apply_async(readFileDigests, args=([job.input_path],)) for job in jobs]

        for p in plist:
            while not p.ready():
                logger.debug("Still waiting for reference digests...")
                time.sleep(5)

            _output = p.get()

            if len(_output) > 0:
                reference_digests = _output
        ee = time.time()
        logger.info("Reference digests created in %s sec." % (int(ee - ss)))


        if step == 0:
            gstart = time.time()
            logger.debug("Start counting %s" % gstart)

        # start the test
        mstart = time.time()
        # CPU_CORES
        digests_matrix = run_execution(faults_tolerate, jobs, reference_digests)
        mend = time.time()
        span = mend - mstart
        logger.info("Execution time (start: %s, end: %s): %s" % (mstart, mend, str(span)))

        logger.info("Return digests: %s" % digests_matrix)

        res = run_verification_global(digests_matrix)

        if res is True:
            logger.info("Step %s completed" % step)
            step += 1

    gend = time.time()
    gspan = str(gend - gstart)
    logger.info("Full execution (start: %s, end: %s): %s" % (gstart, gend, gspan))


if __name__ == "__main__":
    test_run()
