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

import pydoop.hdfs as phdfs

from util import chdir, get_exec, mk_hdfs_temp_dir, setup_logging

logger = setup_logging()

GlobalConf = {
        'job_manager_mem'  : 10000,
        #'task_manager_mem' : 160000,
        #'slots'            : 16,
        'task_manager_mem' : 80000,
        'slots'            : 8,
        'props_filename'   : 'bclconverter.properties',
        'flinkpar'         : 1,
        'jnum'             : 2,
        'tasksPerNode'     : 16,
        'session_wait'     : 45,
        'seqal_nthreads'   : 8,
        'seqal_yarn_cores' : 2,
        'reference_archive': 'hs37d5.fasta.tar',
        'seqal_input_fmt'  : 'prq',
        'seqal_output_fmt' : 'sam',
        'remove_output'    : True, # remove output data as jobs finish, to save space
        }

def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help="path to input data")
    parser.add_argument('output', help="output path")
    parser.add_argument('--n-nodes', type=int, help="number of nodes in the cluster", default=1)
    parser.add_argument('--converter-path', help="bclconverter directory", default='.')
    parser.add_argument('--keep-intermediate', help="Don't delete intermediate data", action='store_true')
    parser.add_argument('--skip-bcl', action='store_true', help="Skip bcl conversion step")
    parser.add_argument('--log-level', choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'),
            help="Logging level", default='INFO')
    return parser

def parse_args(args):
    p = make_parser()
    options = p.parse_args(args)

    if options.n_nodes <= 0:
        p.error("--n-nodes must be >= 1")

    if phdfs.path.exists(options.output):
        p.error("Output path {} already exists".format(options.output))

    if options.keep_intermediate and options.skip_bcl:
        p.error("--keep-intermediate and --skip-bcl are incompatible")

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

    if GlobalConf.get('session_wait', 0) < 0:
        parser.error("session_wait, if present, must be >= 0 (found {})".format(GlobalConf['session_wait']))

    # test whether we can find the executables  we need to run
    for e in ('yarn-session.sh', 'flink', 'seal', 'yarn', 'hdfs'):
        get_exec(e)


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
    def failed(self):
        return self.done and self._retcode != 0

    @property
    def retcode(self):
        if self.done:
            return self._retcode
        else:
            return None

    @property
    def output_path(self):
        return self._output_path

def _parse_session_output(text):
    regex = re.compile(r'\s*yarn application -kill (application_\d+_\d+)\s*')
    for line in text.split('\n'):
        if line.startswith('yarn application -kill application_'):
            m = regex.match(line)
            if m:
                return m.group(1)
    logger.debug("Unable to extract app id from output of yarn-session.sh")
    logger.debug("here's the output:\n%s", text)
    raise RuntimeError("Unable to extract Yarn application id from output of yarn-session.sh")

def _get_app_status(app_id):
    valid_states = set([
        'ALL', 'NEW', 'NEW_SAVING', 'SUBMITTED',
        'ACCEPTED', 'RUNNING', 'FINISHED', 'FAILED', 'KILLED', 'UNDEFINED' ])
    cmd = [ 'yarn', 'application', '-status', app_id ]
    output = subprocess.check_output(cmd)
    state = None
    final_state = None
    for line in output.split('\n'):
        key, value = line.strip().split(':')
        if key.strip().lower() == 'state':
            state = value.strip().upper()
        if key.strip().lower() == 'final-state':
            final_state = value.strip().upper()
        if state and final_state:
            if state not in valid_states:
                raise ValueError("Unrecognized application state '{}'".format(state))
            if final_state not in valid_states:
                raise ValueError("Unrecognized application final state '{}'".format(final_state))
            return state, final_state
    raise RuntimeError("Unable to get status of application {}".format(app_id))


def _start_flink_yarn_session(n_nodes):
    """
    :return: yarn application id of the session
    """
    cmd = [ get_exec('yarn-session.sh'),
             '-n',  n_nodes * 2,
             '-jm', GlobalConf['job_manager_mem'], # job manager memory
             '-tm', GlobalConf['task_manager_mem'], # task manager memory
             '-s',  GlobalConf['slots'],
             '-d', # run in detached mode
          ]
    logger.info("Starting flink session on Yarn in detached mode")
    logger.info("Configuration:\n\tnodes: %d\n\tjm mem: %d\n\ttm mem: %d\n\tslots: %d",
            n_nodes, GlobalConf['job_manager_mem'], GlobalConf['task_manager_mem'], GlobalConf['slots'])
    logger.debug("executing command: %s", cmd)
    try:
        output = subprocess.check_output(map(str, cmd))
    except subprocess.CalledProcessError:
        logger.error("Failed to start Flink session on Yarn!")
        raise

    logger.debug(
            "Session output\n============================================================\n"
            "%s\n============================================================", output)
    app_id = _parse_session_output(output)
    logger.info("Flink session started with application id '%s'", app_id)
    state, final_state = _get_app_status(app_id)

    while state != 'RUNNING' and final_state == 'UNDEFINED':
        logger.debug("Waiting for session to enter the RUNNING state (currently in %s)", state)
        time.sleep(2)
        state, final_state = _get_app_status(app_id)

    if final_state != 'UNDEFINED':
        raise RuntimeError("Problem!! Flink session {} has terminated!  Final state: {}".format(app_id, final_state))

    logger.info("Flink session %s RUNNING", app_id)

    if GlobalConf.get('session_wait', 0) > 0:
        logger.info("Waiting for %d seconds to flink session to start TaskManagers",
                GlobalConf['session_wait'])
        time.sleep(GlobalConf['session_wait'])
        logger.debug("Wait finished.")

    return app_id

def _run_converter_wo_yarn_session(input_dir, output_dir, n_nodes, jar_path):
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
        cmd = [ get_exec("flink"), "run",
                "-c", "bclconverter.bclreader.test", # class name
                jar_path ]
        logger.debug("executing command: %s", cmd)
        with chdir(run_dir):
            logger.debug("Now running flink")
            subprocess.check_call(map(str, cmd), cwd=run_dir)
    finally:
        logger.debug("Removing run directory %s", run_dir)
        try:
            shutil.rmtree(run_dir)
        except IOError as e:
            logger.debug("Error cleaning up temporary dir %s", run_dir)
            logger.debug(e.message)


def _run_converter_and_yarn_session(input_dir, output_dir, n_nodes, jar_path):
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
        cmd = [ get_exec("flink"), "run",
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

def _tear_down_flink_session(app_id):
    if not app_id:
        raise ValueError("_tear_down_flink_session: empty app id!")

    cmd = [ 'yarn', 'application', '-kill', app_id ]
    logger.info("Killing flink session with app id '%s'", app_id)
    logger.debug("Command: %s", cmd)
    subprocess.check_call(cmd)
    # clean up temporary yarn session files, if any
    path = ".flink/" + app_id
    if phdfs.path.exists(path):
        logger.info("Also removing the session's temporary files in %s", path)
        phdfs.rmr(path)

def _clean_up_bcl_output(output_dir):
    """
    Delete prq files with no data
    """
    host, port, _ = phdfs.path.split(output_dir)
    fs = phdfs.hdfs(host, port)
    count = 0
    for item in fs.walk(output_dir):
        if item['kind'] == 'file' and item['name'].endswith('.gz') and item['size'] < 30:
            if not item['name'].startswith('hdfs://'):
                raise RuntimeError("Insanity!  Tring to delete %s!", item['name'])
            fs.delete(item['name'], recursive=False)
            count += 1
    logger.info("Removed %d empty files from bcl output", count)

    undet_path = os.path.join(output_dir, 'Undetermined')
    if phdfs.path.exists(undet_path):
        logger.info("Removing reads from Undetermined dataset %s", undet_path)
        fs.delete(undet_path)


def run_bcl_converter(input_dir, output_dir, n_nodes, jar_path):
    if n_nodes < 1:
        raise ValueError("n_nodes must be >= 1 (got {})".format(n_nodes))

    logger.info("Starting flink session on Yarn")
    app_id = _start_flink_yarn_session(n_nodes)
    try:
        logger.info("Running flink bcl converter")
        _run_converter_wo_yarn_session(input_dir, output_dir, n_nodes, jar_path)
        _clean_up_bcl_output(output_dir)
    finally:
        try:
            _tear_down_flink_session(app_id)
        except StandardError as e:
            logger.error("Problem tearing down flink session")
            logger.exception(e)

def _get_samples_from_bcl_output(output_dir):
    def name_filter(n):
        bn = os.path.basename(n).lower()
        return not (bn.startswith('_') or ('unknown' in bn) or ('undetermined' in bn) )

    return [
            d['name'] for d in phdfs.lsl(output_dir)
            if d['kind'] == 'directory' and name_filter(d['name']) ]

def _try_remove_hdfs_dir(path):
    try:
        phdfs.rmr(path)
        return True
    except StandardError as e:
        logger.error("Error while trying to remove directory %s", path)
        logger.exception(e)
    return False

def _yarn_get_app_ids():
    yarn_exec = get_exec('yarn')
    yarn_output = subprocess.check_output([ yarn_exec, 'application', '-list' ])
    app_ids = [ line.split('\t', 1)[0] for line in yarn_output.split('\n')[2:] ]
    return app_ids

def _yarn_kill_all_apps():
    error = False
    yarn_exec = get_exec('yarn')
    for app_id in _yarn_get_app_ids():
        cmd = [ yarn_exec, 'application', '-kill', app_id ]
        logger.debug("killing application %s: %s", app_id, cmd)
        retcode = subprocess.call(cmd)
        if retcode != 0:
            logger.info("Failed to kill yarn application %s", app_id)
            error = True
    if error:
        raise RuntimeError("Failed to kill some running yarn applications")

def _wait(jobs, remove_output):
    logger.info("Waiting for jobs to finish")
    running = list(jobs)
    secs = 0
    poll_freq = 2
    failed = False
    while running and not failed:
        failed = any( (j.failed for j in running ) )
        if failed:
            break
        # update running list
        new_running = []
        for j in running:
            if j.done: # job just finished
                logger.info("Alignment job writing to %s just finished", phdfs.path.basename(j.output_path))
                if remove_output:
                    logger.info("Removing output path %s", j.output_path)
                    _try_remove_hdfs_dir(j.output_path)
            else:
                new_running.append(j)
        running = new_running
        if secs % 8 == 0:
            logger.info("%d jobs (out of %d) haven't finished", len(running), len(jobs))
        if secs % 60 == 0:
            logger.debug("Logging free disk space situation")
            subprocess.call([get_exec('hdfs'), 'dfsadmin', '-report'])
        if running:
            time.sleep(poll_freq)
            secs += poll_freq
    if failed:
        logger.error("We have failed jobs :-(")
        logger.error("Killing  all remaining jobs on Yarn cluster")
        try:
            _yarn_kill_all_apps()
        except StandardError as e:
            logger.error("Failed to clean up yarn cluster.  Sorry!")
            logger.exception(e)
    else:
        logger.info("All jobs finished")

    return not failed

def run_alignments(bcl_output_dir, output_dir):
    sample_directories = _get_samples_from_bcl_output(bcl_output_dir)
    logger.info("Found %d samples in bcl output directory", len(sample_directories))
    logger.debug("Making base output directory %s", output_dir)
    phdfs.mkdir(output_dir)
    # launch all the jobs
    base_cmd = [
            get_exec('seal'), 'seqal', '--align-only',
            '-D', 'seal.seqal.nthreads={:d}'.format(GlobalConf['seqal_nthreads']),
            '-D', 'mapreduce.map.cpu.vcores={:d}'.format(GlobalConf['seqal_yarn_cores']),
            '--input-format', GlobalConf.get('seqal_input_fmt', 'prq'),
            '--output-format', GlobalConf.get('seqal_output_fmt', 'sam'),
            '--ref-archive', GlobalConf['reference_archive'],
        ]
    def start_job(sample_dir):
        sample_output_dir = phdfs.path.join(output_dir, os.path.basename(sample_dir))
        cmd = base_cmd + [ sample_dir, sample_output_dir ]
        # LP: should refactor to start the job within the AlignJob object
        job = AlignJob(cmd=cmd, inputp=sample_dir, outputp=sample_output_dir)
        logger.info("Launching alignment of sample %s", os.path.basename(sample_dir))
        logger.debug("executing command: %s", cmd)
        job.popen_obj = subprocess.Popen(map(str, cmd), bufsize=4096)
        job.popen_obj.poll()
        logger.debug("job running with PID %d", job.popen_obj.pid)
        return job

    jobs = [ start_job(s) for s in sample_directories ]
    ok = _wait(jobs, GlobalConf['remove_output'])
    if not ok:
        errored_jobs = [ j for j in jobs if j.failed ]
        logger.error("%d alignment jobs failed", len(errored_jobs))
        logger.error("Here are the return codes: %s", ', '.join([ str(j.retcode) for j in errored_jobs ]))
        raise RuntimeError("Some alignment jobs failed")


def main(args):
    logger.setLevel(logging.DEBUG)

    options = parse_args(args)
    logger.setLevel(options.log_level)

    logger.info("Running workflow with the following configuration")
    logger.info("n_nodes: %d", options.n_nodes)
    logger.info("bcl converter jar %s", options.jar_path)
    logger.info("Other conf:\n%s", GlobalConf)

    start_time = time.time()
    try:
        if options.skip_bcl:
            logger.info("Skipping bcl conversion as requested")
            tmp_output_dir = options.input
        else:
            tmp_output_dir = mk_hdfs_temp_dir('bcl_output_')
            logger.debug("Temporary output directory on HDFS: %s", tmp_output_dir)
            run_bcl_converter(options.input, tmp_output_dir, options.n_nodes, options.jar_path)
        time_after_bcl = time.time()
        run_alignments(tmp_output_dir, options.output)
        time_after_align = time.time()
    finally:
        if options.keep_intermediate:
            logger.info("Leaving intermediate data in directory %s", tmp_output_dir)
        elif not options.skip_bcl: # if we skipped bcl, tmp_conf_dir is the input directory
            try:
                phdfs.rmr(tmp_output_dir)
            except StandardError as e:
                logger.error("Error while trying to remove temporary output directory %s", tmp_output_dir)
                logger.exception(e)

    finish_time = time.time()
    logger.info("Seconds for bcl conversion:  %0.2f", (time_after_bcl - start_time))
    logger.info("Seconds for alignment:  %0.2f", (time_after_align - time_after_bcl))
    logger.info("Total execution time:  %0.2f", (finish_time - start_time))

if __name__ == '__main__':
    main(sys.argv[1:])
