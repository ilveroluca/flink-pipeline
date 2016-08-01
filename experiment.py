#!/usr/bin/env python

import argparse
import logging
import os
import re
import subprocess
import sys
import time

import pydoop.hdfs as phdfs

from util import chdir, setup_logging
logger = setup_logging()

GlobalConf = {
    'sleep_between_runs': 60,
    'workflow_logfile'  : 'workflow.log',
    }

class HdfsWorkflow(object):
    def __init__(self, path_to_exec):
        self._program = path_to_exec
        self._args = [
            '--n-nodes', 1,
            '--converter-path', '/home/admin/code/bclconverter/',
            '--log-level', 'DEBUG',
            'illumina.small',
        ]

    def _get_part_times_from_log(self, logfile):
        bcl_regex = re.compile(r'.*Seconds for bcl conversion:\s*(\d+\.\d+).*')
        align_regex = re.compile(r'.*Seconds for alignment:\s*(\d+\.\d+).*')
        bcl_time = None
        align_time = None

        with open(logfile) as f:
            BLOCK_SIZE = 1024
            f.seek(-BLOCK_SIZE, 2) # we assume these lines will be in last BLOCK_SIZE bytes
            for line in f:
                m = bcl_regex.match(line)
                if m:
                    bcl_time = float(m.group(1))
                else:
                    m = align_regex.match(line)
                    if m:
                        align_time = float(m.group(1))
                if bcl_time is not None and align_time is not None:
                    return bcl_time, align_time
        raise RuntimeError("Failed to get bcl times and alignment times from workflow log")


    def execute(self):
        """
        Execute workflow in dedicated directory
        """
        hdfs_output_dir = "workflow_output_{}".format(time.time())
        logger.debug("Setting up workflow")
        logger.debug("CWD: %s", os.getcwd())
        logger.debug("workflow output directory: %s", hdfs_output_dir)
        cmd = [ self._program ] + [ str(arg) for arg in  self._args ]
        cmd.append(hdfs_output_dir)
        logger.debug("workflow command: %s", cmd)
        wf_logfile = os.path.abspath(GlobalConf['workflow_logfile'])
        logger.info("Executing worflow")
        logger.info("Writing workflow log to %s", wf_logfile)

        try:
            with open(wf_logfile, 'a') as f:
                start_time = time.time()
                retcode = subprocess.call(cmd, stdout=f, stderr=subprocess.STDOUT)
            end_time = time.time()
            run_time = end_time - start_time

            attempt_info = AttemptInfo(cmd, retcode, wf_logfile, run_time)

            if retcode == 0:
                logger.info("Workflow finished")
                logger.info("Attempt took %0.2f seconds", run_time)
                bcl, align = self._get_part_times_from_log(wf_logfile)
                attempt_info.bcl_secs = bcl
                attempt_info.align_secs = align
            else:
                logger.info("Workflow FAILED with exit code %s", retcode)
            return attempt_info
        finally:
            try:
                if phdfs.path.exists(hdfs_output_dir):
                    logger.debug("Removing workflow's temporary output directory %s", hdfs_output_dir)
                    phdfs.rmr(hdfs_output_dir)
            except StandardError as e:
                logger.error("Failed to clean up workflow's output directory  %s", hdfs_output_dir)
                logger.exception(e)


class AttemptInfo(object):
    def __init__(self, cmd, retcode, logfile, total_secs):
        self._cmd = cmd
        self._retcode = retcode
        self._log_file = logfile
        self._total_secs = total_secs
        self._repeat_num = None
        self._attempt_num = None
        self._bcl_secs = None
        self._align_secs = None

    @property
    def successful(self):
        if self._retcode == 0:
            return True
        elif self._retcode is None:
            return None
        else:
            return False

    @property
    def repeat_num(self):
        return self._repeat_num

    @repeat_num.setter
    def repeat_num(self, v):
        if v < 1:
            raise ValueError("Invalid repeat number {}".format(v))
        self._repeat_num = v

    @property
    def attempt_num(self):
        return self._attempt_num

    @attempt_num.setter
    def attempt_num(self, v):
        if v < 1:
            raise ValueError("Invalid attempt number {}".format(v))
        self._attempt_num = v

    @property
    def bcl_secs(self):
        return self._bcl_secs

    @bcl_secs.setter
    def bcl_secs(self, v):
        if v < 0:
            raise ValueError("seconds cannot be negative")
        self._bcl_secs = v

    @property
    def align_secs(self):
        return self._align_secs

    @align_secs.setter
    def align_secs(self, v):
        if v < 0:
            raise ValueError("seconds cannot be negative")
        self._align_secs = v

    def csv_header(self):
        return ','.join(
            (
                'repeat_num',
                'attempt_num',
                'retcode',
                'log_file',
                'total_secs',
                'bcl_secs',
                'align_secs',
            ))

    def to_csv(self):
        return ','.join(
            (
                str(self._repeat_num),
                str(self._attempt_num),
                str(self._retcode),
                self._log_file,
                str(self._total_secs),
                str(self._bcl_secs),
                str(self._align_secs),
            ))

class Experiment(object):
    def __init__(self, workflow, results_dir, num_repeats):
        self._workflow = workflow
        self._results_dir = results_dir
        self._results_filename = os.path.join(results_dir, 'results.csv')
        self._prefix = os.path.basename(self._results_dir)
        self._num_repeats = num_repeats
        self._max_retries = 3
        self._attempts = []

    @property
    def results_dir(self):
        return self._results_dir

    @property
    def max_retries(self):
        return self._max_retries

    @max_retries.setter
    def max_retries(self, v):
        if v < 1:
            raise ValueError("max retries must be >= 1 (got {})".format(v))
        self._max_retries = v

    def _run_attempt(self, rep_num, retry_num):
        if rep_num > 1 and retry_num > 1:
            #  not the first time we run.  Sleep to give time to the yarn cluster to "recuperate"
            logger.info("Sleeping %d seconds between runs", GlobalConf['sleep_between_runs'])
            time.sleep(GlobalConf['sleep_between_runs'])

        run_dir = os.path.join(self._results_dir, "rep_{:d}_attempt_{:d}".format(rep_num, retry_num))
        logger.debug("Making run directory %s for repetition %s / attempt %s", run_dir, rep_num, retry_num)
        os.makedirs(run_dir)

        with chdir(run_dir):
            logger.info("Clearing system caches")
            self._clear_caches()
            logger.info("Starting attempt")
            attempt_info = self._workflow.execute()
        attempt_info.repeat_num = rep_num
        attempt_info.attempt_num = retry_num
        return attempt_info

    def _clear_caches(self):
        cmd = "sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'"
        logger.debug("cmd: %s", cmd)
        subprocess.check_call(cmd, shell=True)

    def _record_attempt(self, attempt_info):
        self._attempts.append(attempt_info)
        if attempt_info.successful:
            logger.info("Attempt ran successfully")
        else:
            logger.error("Attempt %d of repeat %d generated an error",
                         attempt_info.attempt_num, attempt_info.repeat_num)

        first_record = not os.path.exists(self._results_filename)
        with open(self._results_filename, 'a') as f :
            if first_record:
                f.write(attempt_info.csv_header() + '\n')
            f.write("{}\n".format(attempt_info.to_csv()))

    def execute(self):
        logger.debug("Making results directory %s", self._results_dir)
        os.makedirs(self._results_dir)

        for rep in xrange(1, self._num_repeats + 1):
            logger.info("=*=*=*=*=*=*=*=*=*= Starting repeat %d *=*=*=*=*=*=*=*=*=*=*=*=*=*=*", rep)
            retry = 1
            while retry <= self._max_retries:
                if retry > 1:
                    logger.info("Retrying workflow run")
                attempt_info = self._run_attempt(rep, retry)
                self._record_attempt(attempt_info)
                if attempt_info.successful:
                    break
                retry += 1
            if retry > self._max_retries:
                logger.critical("****** Run failed %d times ******", retry - 1)
                logger.critical("We've reached the limit on the number of retries (%d)", self._max_retries)
                logger.critical("Giving up")
                raise RuntimeError("Too many attempts failed on repeat number {:d} ({:d} failures).  Giving up".format(rep, retry - 1))
            logger.info("====================== Finished repeat %d =========================", rep)
        logger.info("Experiment finished")

def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('program', help="The program to execute")
    parser.add_argument('results_dir', help="Where the results and logs will be written. The basename will be used as a prefix")
    parser.add_argument('--max-retries', type=int, default=3,
            help="Max retries in case workflow execution fails")
    parser.add_argument('--num-repeats', type=int, default=3,
            help="Number of times to resample workflow execution")
    return parser

def parse_args(args):
    p = make_parser()
    options = p.parse_args(args)

    if not os.path.exists(options.program):
        raise ValueError("Specified program {} doesn't exist".format(options.program))
    if not os.access(options.program, os.R_OK | os.X_OK):
        raise ValueError("Specified program {} is not an executable".format(options.program))

    if options.max_retries < 1:
        p.error("--max-retries must be >= 1")
    if options.max_retries < 1:
        p.error("--num-repeats must be >= 1")
    if os.path.exists(options.results_dir):
        p.error("Results directory {} already exists".format(options.results_dir))

    return options

def main(args):
    logger.setLevel(logging.DEBUG)
    p = make_parser()
    options = p.parse_args(args)

    logger.debug("Creating Experiment")
    logger.debug("Results dir: %s", os.path.abspath(options.results_dir))
    logger.debug("Num repeats: %d; max retries: %d", options.num_repeats, options.max_retries)
    exp = Experiment(HdfsWorkflow(options.program), os.path.abspath(options.results_dir), options.num_repeats)
    exp.max_retries = options.max_retries

    logger.info("Starting experiment")
    exp.execute()
    logger.info("Experiment finished running")
    logger.info("You'll find the results in %s", exp.results_dir)

if __name__ == '__main__':
    main(sys.argv[1:])
