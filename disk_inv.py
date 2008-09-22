#!/usr/bin/python

import sys
import os
import stat
from types import StringTypes
import hashlib

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
                md5 = md5_file(full_path)
                sb = os.stat(full_path)
                writer.write_file(full_path, md5, mtime=mtime)
        writer.end_dir()

def md5_file(file):
    close_file = False
    if isinstance(file, StringTypes):
        file = open(file, 'r')
        close_file = True

    md5 = hashlib.md5()
    BLOCK_SIZE = 40*1024
    block = file.read(BLOCK_SIZE)
    while block:
        md5.update(block)
        block = file.read(BLOCK_SIZE)

    if close_file:
        file.close()

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
