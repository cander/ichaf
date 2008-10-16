#!/usr/bin/python

import sys
import os
import stat
from types import StringTypes
import hashlib
from subprocess import Popen, PIPE
import tarfile
import zipfile
import StringIO
from datetime import datetime

from disk_db import Volume, DbWriter, session

def inventory_dirs(writer, dir_list):
    """Inventory a list of directories, writing the result to the
       specified destination writer."""
    for d in dir_list:
        inventory(d, writer)

def inventory(root_dir, writer):
    for dirpath, dirs, files in os.walk(root_dir):
        path_prefix = dirpath
        path_prefix = os.path.normpath(path_prefix)
        for filename in files:
            full_path = os.path.join(path_prefix,  filename)
            if not os.access(full_path, os.F_OK):
                print 'File %s is not accessible' % full_path
                continue
            try:
                sbuf = os.stat(full_path)
                if stat.S_ISREG(sbuf[stat.ST_MODE]):
                    mtime = sbuf[stat.ST_MTIME]
                    size = sbuf[stat.ST_SIZE]
                    catalog_file(full_path, full_path, mtime, size, writer)
                    if is_tarfile(full_path):
                        inventory_tarfile(full_path, full_path, writer)
                    elif zipfile.is_zipfile(full_path):
                        inventory_zipfile(full_path, full_path, writer)
            except Exception, err:
                print 'Error processing %s: %s' % (full_path, err)


def catalog_file(full_path, recorded_path, mtime, size, writer):
    """
    Catalog one file in the file system (full_path) under the name
    recorded_path.
    """
    (md5, unc_md5) = md5_filename(full_path)
    mtime = datetime.fromtimestamp(mtime)
    writer.write_file(recorded_path, md5, unc_md5, mtime=mtime, size=size)


def md5_filename(path):
    """Compute regular and uncompressed hashes of a named file."""
    close_file = False
    file = open(path, 'r')
    md5 = md5_file(file)
    file.close()

    # look for a compressed file
    pipe = None
    if path.endswith('.gz'):
        cmd = 'gzip -d < "%s"' % path
        pipe = Popen(cmd, shell=True, bufsize=1024 * 4, stdout=PIPE).stdout
    elif path.endswith('.Z'):
        cmd = 'zcat "%s"' % path
        #print 'uncompress cmd:', cmd
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


def is_tarfile(full_path):
    result = False
    """Is this file a tar file that we're prepared to index?"""
    result = False
    if tarfile.is_tarfile(full_path) or full_path.endswith('.tar.Z'):
        result = True
    else:
        result = False
    return result

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
        cmd = 'zcat "%s"' % full_path
        pipe = Popen(cmd, shell=True, bufsize=4096, stdout=PIPE).stdout
        archive = tarfile.open(full_path, 'r|', pipe)
    else:
        mode = 'r:*'
        archive = tarfile.open(full_path, mode)

    try:
        last_file_name = None
        for file in archive:
            if file.isfile():
                # TODO: look for nested tar files
                mtime = file.mtime
                size = file.size
                extract_path = os.path.join(extract_dir, file.name)
                recorded_path = os.path.join(prefix, file.name)
                # XXX - is extract safe in the presence of absolute paths?
                archive.extract(file, extract_dir)
                if os.access(extract_path, os.F_OK):
                    catalog_file(extract_path, recorded_path, mtime, 
                                 size, writer)

                    os.unlink(extract_path)
                else:
                    print 'Error: failed to extract %s from tar file %s' % \
                          (extract_path, full_path)
                last_file_name = file.name
    except Exception, err:
        print 'Exception inventorying tar file %s - last file was %s: %s' % \
              (full_path, last_file_name, err)

    # XXX - does either the archive or pipe need to be closed?


def inventory_zipfile(full_path, short_path, writer):
    """Inventory a zip file."""
    prefix = os.path.join(os.path.dirname(short_path),
                          '[%s]' % os.path.basename(short_path))
    archive = zipfile.ZipFile(full_path, 'r')

    try:
        for info in archive.infolist():
            file_name = info.filename
            if not file_name.endswith('/'):
                # regular file - not directory
                recorded_path = os.path.join(prefix, file_name)
                size = info.file_size
                (year, month, day, hour, mins, secs) = info.date_time
                mtime = datetime(year, month, day, hour, mins, secs)

                # read the whole file into memory - yikes!
                try:
                    contents = archive.read(file_name)
                    file = StringIO.StringIO(contents)
                    md5 = md5_file(file)
                    file.close()
                    writer.write_file(recorded_path, md5, md5, mtime=mtime, size=size)
                except Exception, err:
                    print 'Exception processing file %s within zip %s: %s' % \
                        (file_name, short_path, err)
            last_file_name = file_name
    except Exception, err:
        print 'Exception inventorying zip file %s - last file was %s: %s' % \
              (full_path, last_file_name, err)
    # XXX - does either the archive need to be closed?



class StdoutWriter(object):
    def __init__(self, volume):
        self.volume = volume

    def write_file(self, full_path, md5):
        #print '%s|%s|%s' % (name, full_path, md5)
        # name = os.path.basename(full_path)
        print '%s %s' % (md5, full_path)

def get_volume(vol_name, expect_empty):
    """Get a Volume object from the database."""
    q = session.query(Volume)
    q = q.filter_by(vol_name=vol_name)
    print q
    result = q.one()

    return result

def db_inventory(vol_name, dir_list):
    vol = get_volume(vol_name, True)
    if vol != None:
        writer = DbWriter(vol)
        inventory_dirs(writer, dir_list)

def main(args):
    cmd = args[1]
    if cmd == 'inventory':
        vol_name = args[2]
        dirs = args[3:]
        db_inventory(vol_name, dirs)
    else:
        print 'Unknown command "%s" - quitting' % cmd

if __name__ == '__main__':
    main(sys.argv)
