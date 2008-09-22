#!/usr/bin/python

import sys
import os
from types import StringTypes
import hashlib

def inventory(root_dir, writer):
    for dirpath, dirs, files in os.walk(root_dir):
        #print 'walk output:', dirpath, dirs, files
        path_prefix = dirpath
        path_prefix = os.path.normpath(path_prefix)
        for filename in files:
            full_path = os.path.join(path_prefix,  filename)
            md5 = md5_file(full_path)
            writer.write_file(filename, full_path, md5)

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





class CatalogWriter(object):
    def __init__(self, volume):
        self.volume = volume

    def write_file(self, name, full_path, md5):
        #print '%s|%s|%s' % (name, full_path, md5)
        print '%s %s' % (md5, full_path)

if __name__ == '__main__':
    writer = CatalogWriter('/tmp')
    inventory(sys.argv[1], writer)
