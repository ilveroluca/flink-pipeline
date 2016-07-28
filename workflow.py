#!/usr/bin/env python

import argparse
import fnmatch
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import tempfile
from contextlib import contextmanager

import pydoop.hdfs as phdfs

logging.basicConfig()
logger = logging.getLogger()

GlobalConf = {
        'job_manager_mem'  : 10000,
        'task_manager_mem' : 160000,
        'slots'            : 16,
        'props_filename'   : 'bclconverter.properties',
        'flinkpar'         : 1,
        'jnum'             : 2,
        'tasksPerNode'     : 16,
        'seqal_nthreads'   : 15,
        'reference_archive': 'hs37d5.fasta.tar',
        'seqal_input_fmt'  : 'prq',
        'seqal_output_fmt' : 'sam',
        }


@contextmanager
def chdir(new_dir):
    current_dir = os.getcwd()
    try:
        os.chdir(new_dir)
        yield
    finally:
        os.chdir(current_dir)

def _get_exec(name):
    for dirname in os.getenv('PATH').split(os.pathsep):
        e = os.path.join(dirname, name)
        if os.path.exists(e) and os.access(e, os.X_OK | os.R_OK):
            return e
    raise ValueError("Couldn't find executable {} in the PATH".format(name))

def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help="path to input data")
    parser.add_argument('output', help="output path")
    parser.add_argument('--n-nodes', type=int, help="number of nodes in the cluster", default=1)
    parser.add_argument('--converter-path', help="bclconverter directory", default='.')
    parser.add_argument('--keep-intermediate', help="Don't delete intermediate data", action='store_true')
    parser.add_argument('--log-level', choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'),
            help="Logging level", default='INFO')
    return parser

def parse_args(args):
    p = make_parser()
    options = p.parse_args(args)

    if options.n_nodes <= 0:
        p.error("--n-nodes must be >= 1")

    # check bcl converter path
    if not options.converter_path:
        p.error("--converter-path cannot be an empty argument")
    bcl_dir = os.path.abspath(options.converter_path)
    jar_path = None
    def jar_search(root, files):
        jar_files = fnmatch.filter(files, "bcl-converter-assembly*.jar")
        if len(jar_files) == 0:
            return None
        elif len(jar_files) == 1:
            return os.path.join(root, jar_files[0])
        else:
            raise RuntimeError("BUG!!  Found more than one bcl*.jar file -> {}".format(jar_files))

    for root, _, files in os.walk(bcl_dir, followlinks=True):
        jar_path = jar_search(root, files)
        if jar_path:
            break

    if not jar_path:
        p.error("Can't find bclconverter jar under {}".format(bcl_dir))

    # we stick attach this information to the options object
    options.jar_path = jar_path

    try:
        log_level = getattr(logging, options.log_level)
        options.log_level = log_level # overwrite the existing value
    except AttributeError as e:
        # this should never happend since we restricted the valid
        # choices at the level of the argument parser
        p.error("Invalid log level! " + e.message)

    verify_conf(p)

    return options

def verify_conf(parser):
    # Verify whatever preconditions we can verify
    ref = GlobalConf['reference_archive']
    if not (ref.endswith('.tar.gz') or ref.endswith('.tar.bz2') or ref.endswith('.tar')):
        parser.error("Reference {} doesn't seem to be an archive!".format(ref))
    if not phdfs.path.exists(ref):
        parser.error("Reference {} doesn't seem to exist".format(ref))

    if GlobalConf['job_manager_mem'] <= 100:
        parser.error("job_manager_mem of {:d} is too low".format(GlobalConf['job_manager_mem']))

    if GlobalConf['task_manager_mem'] <= 1000:
        parser.error("task_manager_mem of {:d} is too low".format(GlobalConf['task_manager_mem']))

    # test whether we can find the executables  we need to run
    for e in ('yarn-session.sh', 'flink', 'seal'):
        _get_exec(e)

def mk_temp_dir():
    found = True
    while found:
        tmp = os.path.basename(tempfile.mktemp(prefix='bcl_output_'))
        found = phdfs.path.exists(tmp)
    phdfs.mkdir(tmp)
    return tmp



class AlignJob(object):
    def __init__(self, cmd=None, inputp=None, outputp=None, popen_obj=None):
        self._cmd = cmd
        self._input_path = inputp
        self._output_path = outputp
        self._popen = popen_obj
        self._retcode = None

    @property
    def popen_obj(self):
        return self._popen

    @popen_obj.setter
    def popen_obj(self, v):
        self._popen = v

    @property
    def done(self):
        if self._retcode is None:
            self._retcode = self._popen.poll()
        return self._retcode is not None

    @property
    def retcode(self):
        if self.done:
            return self._retcode
        else:
            return None


def _start_flink_yarn_session(n_nodes):
    cmd = [ _get_exec('yarn-session.sh'),
             '-n',  n_nodes,
             '-jm', GlobalConf['job_manager_mem'], # job manager memory
             '-tm', GlobalConf['task_manager_mem'], # task manager memory
             '-s',  GlobalConf['slots'],
          ]
    logger.debug("executing command: %s", cmd)
    p_session = subprocess.Popen(map(str, cmd), bufsize=4096)
    logger.info("Waiting a bit while yarn-session starts up")
    time.sleep(10)
    p_session.poll()
    if p_session.returncode is not None:
        logger.error("Failed to start Flink session on Yarn!")
        logger.error("return code: %s", p_session.returncode)
        raise RuntimeError("Failed to start Flink session on Yarn (ret code: {})"
                .format(p_session.returncode))

    return p_session

def _run_converter(input_dir, output_dir, n_nodes, jar_path):
    # setup properties file
    run_dir = tempfile.mkdtemp(prefix="bclconverter_run_dir")
    try:
        ## start by preparing the properties file (at the moment the program
        # doesn't accept command line arguments
        tmp_conf_dir = os.path.join(run_dir, "conf")
        os.makedirs(tmp_conf_dir)
        props_file = os.path.join(tmp_conf_dir, GlobalConf['props_filename'])
        with open(props_file, 'w') as f:
            f.write("root = {}/\n".format(input_dir.rstrip('/')))
            f.write("fout = {}/\n".format(output_dir.rstrip('/')))
            f.write("numTasks = {:d}\n".format(GlobalConf['tasksPerNode'] * n_nodes))
            f.write("flinkpar = {:d}\n".format(GlobalConf['flinkpar']))
            f.write("jnum = {:d}\n".format(GlobalConf['jnum']))

        logger.info("Wrote properties in file %s", props_file)
        if logger.isEnabledFor(logging.DEBUG):
            with open(props_file) as f:
                logger.debug("\n=============================\n%s\n=====================\n", f.read())
        # now run the program
        logger.debug("Running flink cwd %s", run_dir)
        cmd = [ _get_exec("flink"), "run",
                "-m", "yarn-cluster",
                 '-yn',  n_nodes,
                 '-yjm', GlobalConf['job_manager_mem'], # job manager memory
                 '-ytm', GlobalConf['task_manager_mem'], # task manager memory
                 '-ys',  GlobalConf['slots'],
                "-c", "bclconverter.bclreader.test", # class name
                jar_path ]
        logger.debug("executing command: %s", cmd)
        with chdir(run_dir):
            logger.debug("In CWD, where we're going to run flink")
            logger.debug("cat conf/bclconverter.properties gives:")
            subprocess.check_call("cat conf/bclconverter.properties", shell=True)
            logger.debug("Now running flink")
            subprocess.check_call(map(str, cmd), cwd=run_dir)
    finally:
        logger.debug("Removing run directory %s", run_dir)
        try:
            shutil.rmtree(run_dir)
        except IOError as e:
            logger.debug("Error cleaning up temporary dir %s", run_dir)
            logger.debug(e.message)

def _get_running_flink_sessions():
    listing = subprocess.check_output("yarn application -list -appStates RUNNING", shell=True)
    data_lines = listing.split('\n')[2:] # remove the 2 header lines as well
    app_ids = [ line.split('\t', 1)[0] for line in data_lines if "Flink session" in line ]
    return [ a for a in app_ids if re.match(r'application_\d+_\d+', a) ] # sanity check

def _kill_flink_yarn_session(popen_yarn):
    logger.info("Terminating Flink/Yarn session")
    popen_yarn.send_signal(signal.SIGINT)
    time.sleep(2)
    if popen_yarn.poll() is not None:
        # not terminated yet.  Try killing
        popen_yarn.kill()

    # kill the application on yarn, if any
    app_ids = _get_running_flink_sessions()
    logger.debug("Found this/these %d flink sessions running on yarn:  %s", len(app_ids), app_ids)
    logger.info("killing running flink sessions")
    for app in app_ids:
        subprocess.check_call("yarn application -kill " + app, shell=True)


def run_bcl_converter(input_dir, output_dir, n_nodes, jar_path):
    if n_nodes < 1:
        raise ValueError("n_nodes must be >= 1 (got {})".format(n_nodes))

    logger.info("Running flink bcl converter")
    _run_converter(input_dir, output_dir, n_nodes, jar_path)


def _get_samples_from_bcl_output(output_dir):
    def name_filter(n):
        bn = os.path.basename(n).lower()
        return not (bn.startswith('_') or 'unknown' in bn)

    return [
            d['name'] for d in phdfs.lsl(output_dir)
            if d['kind'] == 'directory' and name_filter(d['name']) ]

def _wait(jobs):
    logger.info("Waiting for jobs to finish")
    running = list(jobs)
    n = 0
    while running:
        running = [ j for j in running if not j.done ]
        if n % 8 == 0:
            logger.info("%d jobs (out of %d) haven't finished", len(running), len(jobs))
            n = 1
        if running:
            time.sleep(2)
    logger.info("All jobs finished")
    # no need to return anything.  The AlignJob objects passed in through the
    # `jobs` list contain the return code


def run_alignments(bcl_output_dir, output_dir):
    sample_directories = _get_samples_from_bcl_output(bcl_output_dir)
    logger.info("Found %d samples in bcl output directory", len(sample_directories))
    logger.debug("Making base output directory %s", output_dir)
    phdfs.mkdir(output_dir)
    # launch all the jobs
    base_cmd = [
            _get_exec('seal'), 'seqal', '--align-only',
            '-D', 'seal.seqal.nthreads={:d}'.format(GlobalConf['seqal_nthreads']),
            '-D', 'mapreduce.map.cpu.vcores={:d}'.format(GlobalConf['seqal_nthreads']),
            '--input-format', GlobalConf.get('seqal_input_fmt', 'prq'),
            '--output-format', GlobalConf.get('seqal_output_fmt', 'sam'),
            '--ref-archive', GlobalConf['reference_archive'],
        ]
    def start_job(sample_dir):
        sample_output_dir = phdfs.path.join(output_dir, os.path.basename(sample_dir))
        cmd = base_cmd + [ sample_dir, sample_output_dir ]
        job = AlignJob(cmd=cmd, inputp=sample_dir, outputp=sample_output_dir)
        logger.info("Launching alignment of sample %s", os.path.basename(sample_dir))
        logger.debug("executing command: %s", cmd)
        job.popen_obj = subprocess.Popen(map(str, cmd), bufsize=4096)
        job.popen_obj.poll()
        logger.debug("job running with PID %d", job.popen_obj.pid)
        return job

    jobs = [ start_job(s) for s in sample_directories ]
    _wait(jobs)
    errored_jobs = [ j for j in jobs if j.retcode != 0 ]
    if errored_jobs:
        logger.error("%d alignment jobs failed", len(errored_jobs))
        logger.error("Here are the return codes: %s", ', '.join([ str(j.retcode) for j in errored_jobs]))
        raise RuntimeError("Some alignment jobs failed")


def main(args):
    logger.setLevel(logging.DEBUG)

    options = parse_args(args)
    logger.setLevel(options.log_level)

    logger.info("Running workflow with the following configuration")
    logger.info("n_nodes: %d", options.n_nodes)
    logger.info("bcl converter jar %s", options.jar_path)
    logger.info("Other conf:\n%s", GlobalConf)

    tmp_output_dir = mk_temp_dir()
    logger.debug("Temporary output directory on HDFS: %s", tmp_output_dir)
    try:
        run_bcl_converter(options.input, tmp_output_dir, options.n_nodes, options.jar_path)
        run_alignments(tmp_output_dir, options.output)
    finally:
        if options.keep_intermediate:
            logger.info("Leaving intermediate data in directory %s", tmp_output_dir)
        else:
            try:
                phdfs.rmr(tmp_output_dir)
            except StandardError as e:
                logger.error("Error while trying to remove temporary output directory {}".format(e))
                logger.exception(e)

if __name__ == '__main__':
    main(sys.argv[1:])
