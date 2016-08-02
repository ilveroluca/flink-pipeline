
import logging
import os
import subprocess
import tempfile
from contextlib import contextmanager

import pydoop.hdfs as phdfs


def setup_logging(log_level=None):
    logformat = '%(asctime)s\t%(levelname)s\tWORKFLOW\t%(message)s'
    logging.basicConfig(format=logformat)
    logger = logging.getLogger()
    if log_level:
        logger.setLevel(log_level)
    return logger

@contextmanager
def chdir(new_dir):
    current_dir = os.getcwd()
    try:
        os.chdir(new_dir)
        yield
    finally:
        os.chdir(current_dir)

def get_exec(name):
    for dirname in os.getenv('PATH').split(os.pathsep):
        e = os.path.join(dirname, name)
        if os.path.exists(e) and os.access(e, os.X_OK | os.R_OK):
            return e
    raise ValueError("Couldn't find executable {} in the PATH".format(name))

def mk_hdfs_temp_dir(prefix):
    found = True
    while found:
        tmp = os.path.basename(tempfile.mktemp(prefix=prefix))
        found = phdfs.path.exists(tmp)
    phdfs.mkdir(tmp)
    return tmp

def yarn_get_node_list():
    output = subprocess.check_output([get_exec('yarn'), 'node', '-list', '-all'])
    lines = output.split('\n')[2:]
    nodes = [ line.split('\t', 1)[0] for line in lines if line ]
    return nodes

