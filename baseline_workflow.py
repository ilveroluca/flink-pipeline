#!/usr/bin/env python


import argparse
import glob
import logging
import os
import shutil
import subprocess
import sys
import time
import tempfile
from collections import defaultdict

from util import setup_logging, get_exec

logger = setup_logging()

GlobalConf = {
        'bwa_nthreads'     : 31,
        'reference_path'   : '/data/data01/hs37d5/hs37d5.fasta',
        'tmp_space'        : '/data/data02/',
        }

def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help="path to input data")
    parser.add_argument('output', help="output path")
    parser.add_argument('--converter-path', help="Path to Illumina's bcl2fastq executable.  If not set we look for it in the PATH")
    parser.add_argument('--bwa-path', help="Path to bwa executable.  If not set we look for it in the PATH")
    parser.add_argument('--keep-intermediate', help="Don't delete intermediate data", action='store_true')
    parser.add_argument('--log-level', choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'),
            help="Logging level", default='INFO')
    return parser

def parse_args(args):
    p = make_parser()
    options = p.parse_args(args)

    # check bcl converter and bwa path
    if options.converter_path:
        if not os.path.exists(options.converter_path):
            p.error("Specified converter doesn't exist")
        if not os.access(options.converter_path, os.X_OK | os.R_OK):
            p.error("Specified converter is not executable")
    else:
        options.converter_path = get_exec('bcl2fastq')

    if options.bwa_path:
        if not os.path.exists(options.bwa_path):
            p.error("Specified bwa doesn't exist")
        if not os.access(options.bwa_path, os.X_OK | os.R_OK):
            p.error("Specified bwa is not executable")
    else:
        options.bwa_path = get_exec('bwa')

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
    ref = GlobalConf['reference_prefix']
    parts = glob.glob(ref + '*')
    if not parts:
        parser.error("Reference {} doesn't seem to exist".format(ref))

    if GlobalConf['bcl_nthreads'] <= 0:
        parser.error("Value of bcl_nthreads configuration must be > 0 (found {})".format(GlobalConf['bcl_nthreads']))
    if GlobalConf['bwa_nthreads'] <= 0:
        parser.error("Value of bwa_nthreads configuration must be > 0 (found {})".format(GlobalConf['bwa_nthreads']))

def run_bcl_converter(input_dir, output_dir, exec_path):
    logger.info("Running illumina bcl converter")

    cmd = [ exec_path,
            '--runfolder-dir', input_dir, '--output-dir', output_dir,
            '--fastq-compression-level', 1,
          ]
    logger.debug("executing command: %s", cmd)
    subprocess.check_call(map(str, cmd))

def _get_samples_from_bcl_output(output_dir):
    """
    Returns a dictionary 'sample name' -> ( [read1 files], [read2 files])
    """
    def name_filter(n):
        bn = os.path.basename(n).lower()
        return bn.endswith('.fastq.gz') and not bn.startswith('undetermined')

    i_files = ( os.path.abspath(f) for f in os.listdir(output_dir) if name_filter(f) and os.path.isfile(f))

    samples = defaultdict(lambda : (list(), list()))
    for fname in i_files:
        # filenames look like this: DNA16-0084-R0001_S13_L003_R2_001.fastq.gz
        parts = fname.split('_')
        if parts[3] == 'R1':
            read = 1
        elif parts[3] == 'R2':
            read = 2
        else:
            raise ValueError("Unrecognized read number {} in filename {}".format(parts[3], fname))
        samples[parts[0]][read].append(fname)

    return samples

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

def _run_bwa(sample_file_lists, ref, output, bwa_path, nthreads):
    r1_files, r2_files = sample_file_lists
    logger.debug("r1 files: %s", r1_files)
    logger.debug("r2 files", r2_files)

    cmd = "{} -t {:d} -T 0 {} <(gunzip -c {}) <(gunzip -c {}) > {}".format(
            bwa_path, nthreads, ref, ' '.join(r1_files), ' '.join(r2_files), output)

    logger.debug("Executing cmd: %s", cmd)
    subprocess.check_call([cmd], shell=True)


def run_alignments(bcl_output_dir, output_dir, reference, bwa_path, nthreads):
    samples = _get_samples_from_bcl_output(bcl_output_dir)
    logger.info("Found %d samples in bcl output directory", len(samples))
    logger.debug("Making base output directory %s", output_dir)
    os.makedirs(output_dir)
    logger.info("Starting alignment jobs")

    for name, file_lists in samples.iteritems():
        logger.info("Aligning sample %s", name)
        output_file = os.path.join(output_dir, name + '.sam')
        try:
            _run_bwa(file_lists, reference, output_file, bwa_path, nthreads)
        except subprocess.CalledProcessError as e:
            logger.error("Failed to align sample %s with files %s", name, file_lists)
            logger.exception(e)
            raise
        logger.info("Finished aligning %s.  Removing output file %s", name, output_file)
        os.remove(output_file)

def main(args):
    logger.setLevel(logging.DEBUG)

    options = parse_args(args)
    logger.setLevel(options.log_level)

    logger.info("Running workflow with the following configuration")
    logger.info("Options:\n%s", options)
    logger.info("Other conf:\n%s", GlobalConf)

    bcl_output_dir = tempfile.mkdtemp(prefix="bcl_output_", dir=GlobalConf.get('tmp_space'))
    logger.debug("Output directory for bcl conversion: %s", bcl_output_dir)
    start_time = time.time()
    try:
        run_bcl_converter(options.input, bcl_output_dir, options.converter_path)
        time_after_bcl = time.time()
        run_alignments(bcl_output_dir, options.output,
                GlobalConf['reference_path'], options.bwa_path, GlobalConf['bwa_nthreads'])
        time_after_align = time.time()
    finally:
        if options.keep_intermediate:
            logger.info("Leaving intermediate data in directory %s", bcl_output_dir)
        else:
            try:
                shutil.rmtree(bcl_output_dir)
            except StandardError as e:
                logger.error("Error while trying to remove temporary output directory {}".format(e))
                logger.exception(e)

    finish_time = time.time()
    logger.info("Seconds for bcl conversion:  %0.2f", (time_after_bcl - start_time))
    logger.info("Seconds for alignment:  %0.2f", (time_after_align - time_after_bcl))
    logger.info("Total execution time:  %0.2f", (finish_time - start_time))

if __name__ == '__main__':
    main(sys.argv[1:])
