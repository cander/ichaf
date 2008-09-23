#!/usr/bin/python

import sys
import os
import stat
from types import StringTypes
import hashlib
from subprocess import Popen, PIPE

from disk_db import File, DbWriter

def inventory(root_dir, writer):
    for dirpath, dirs, files in os.walk(root_dir):
        #print 'walk output:', dirpath, dirs, files
        path_prefix = dirpath
        path_prefix = os.path.normpath(path_prefix)
        for filename in files:
            full_path = os.path.join(path_prefix,  filename)
            sbuf = os.stat(full_path)
            if  stat.S_ISREG(sbuf[stat.ST_MODE]):
                mtime = sbuf[stat.ST_MTIME]
                (md5, unc_md5) = md5_filename(full_path)
                sb = os.stat(full_path)
                writer.write_file(full_path, md5, unc_md5, mtime=mtime)
        writer.end_dir()

def md5_filename(path):
    """Compute regular and uncompressed hashes of a named file."""
    close_file = False
    file = open(path, 'r')
    md5 = md5_file(file)
    file.close()

    # look for a compressed file
    pipe = None
    if path.endswith('.gz'):
        cmd = 'gzip -d < %s' % path
        print 'uncompress cmd:', cmd
        pipe = Popen(cmd, shell=True, bufsize=1024 * 4, stdout=PIPE).stdout
    # else compress, etc

    if pipe:
        uncompressed_md5 = md5_file(pipe)
        # XXX - should check the exit status
    else:
        uncompressed_md5 = md5

    return (md5, uncompressed_md5)



def md5_file(file):
    """Compute an MD5 hash on a file (like) object."""
    md5 = hashlib.md5()
    BLOCK_SIZE = 64*1024
    block = file.read(BLOCK_SIZE)
    while block:
        md5.update(block)
        block = file.read(BLOCK_SIZE)

    return md5.hexdigest()





class StdoutWriter(object):
    def __init__(self, volume):
        self.volume = volume

    def write_file(self, full_path, md5):
        #print '%s|%s|%s' % (name, full_path, md5)
        # name = os.path.basename(full_path)
        print '%s %s' % (md5, full_path)

if __name__ == '__main__':
    #writer = StdoutWriter('/tmp')
    writer = DbWriter('/tmp')
    inventory(sys.argv[1], writer)
