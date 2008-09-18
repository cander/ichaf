#!/usr/bin/python

import sys
import os

def inventory(root_dir, writer):
    os.chdir(root_dir)
    for dirpath, dirs, files in os.walk('.'):
        print 'walk output:', dirpath, dirs, files
        path_prefix = os.path.join(root_dir, dirpath)
        path_prefix = os.path.normpath(path_prefix)
        for filename in files:
            full_path = os.path.join(path_prefix,  filename)
            writer.write_file(filename, full_path, '0x123')


class CatalogWriter(object):
    def __init__(self, volume):
        self.volume = volume

    def write_file(self, name, full_path, md5):
        print '%s|%s|%s' % (name, full_path, md5)

if __name__ == '__main__':
    writer = CatalogWriter('/tmp')
    inventory(sys.argv[1], writer)
