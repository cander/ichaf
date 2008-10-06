#!/usr/bin/python

import sys
import os
import stat
from types import StringTypes
import hashlib
from subprocess import Popen, PIPE
import tarfile

from disk_db import File, DbWriter

def inventory(root_dir, writer):
    for dirpath, dirs, files in os.walk(root_dir):
        #print 'walk output:', dirpath, dirs, files
        path_prefix = dirpath
        path_prefix = os.path.normpath(path_prefix)
        writer.begin_dir(path_prefix)
        for filename in files:
            full_path = os.path.join(path_prefix,  filename)
            sbuf = os.stat(full_path)
            if  stat.S_ISREG(sbuf[stat.ST_MODE]):
                mtime = sbuf[stat.ST_MTIME]
                catalog_file(full_path, full_path, mtime, writer)
                #(md5, unc_md5) = md5_filename(full_path)
                #writer.write_file(full_path, md5, unc_md5, mtime=mtime)
                if is_tarfile(filename):
                    inventory_tarfile(full_path, full_path, writer)
        writer.end_dir()

def catalog_file(full_path, recorded_path, mtime, writer):
    """
    Catalog one file in the file system (full_path) under the name
    recorded_path.
    """
    (md5, unc_md5) = md5_filename(full_path)
    writer.write_file(recorded_path, md5, unc_md5, mtime=mtime)


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


def is_tarfile(filename):
    """Is this file a tar file that we're prepared to index?"""
    # XXX - only deal with tar files, right now
    extensions = [ '.tar', '.tar.gz', '.tgz', '.tar.Z' ]
    for ext in extensions:
        if filename.endswith(ext):
            return True
    return False

def inventory_tarfile(full_path, short_path, writer):
    """Inventory a tar file."""
    extract_dir = '/tmp/extract'    #XXX - hardcoded
    #prefix = '%s/[%s]' % (os.path.dirname(short_path),
    #                      os.path.basename(short_path))
    prefix = os.path.join(os.path.dirname(short_path),
                          '[%s]' % os.path.basename(short_path))
    # XXX - assume tar files, right now
    # figure out the right mode and type of archiver
    if full_path.endswith('.tar.Z'):
        cmd = 'zcat %s' % full_path
        pipe = Popen(cmd, shell=True, bufsize=4096, stdout=PIPE).stdout
        archive = tarfile.open(full_path, 'r|', pipe)
    else:
        mode = 'r:*'
        archive = tarfile.open(full_path, mode)

    for file in archive:
        if file.isfile():
            # look for nexted tar files
            mtime = file.mtime
            extract_path = os.path.join(extract_dir, file.name)
            recorded_path = os.path.join(prefix, file.name)
            # XXX - is extract safe in the presence of absolute paths?
            archive.extract(file, extract_dir)
            print 'extracted', extract_path, os.path.exists(extract_path)
            catalog_file(extract_path, recorded_path, mtime, writer)
            os.unlink(extract_path)
    writer.end_dir()
    # XXX - does either the archive or pipe need to be closed?



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
