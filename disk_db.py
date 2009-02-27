import os
import sys
from sqlalchemy import *
from sqlalchemy.orm import *
from datetime import datetime

metadata = MetaData('sqlite:///disks.db')
#metadata.bind.echo = True

volume_table = Table(
    'volume', metadata,
    Column('id',                Integer,        primary_key=True),
    Column('vol_name',          String(60),        ),
    Column('notes',             String,        ),
    Column('mtime',             DateTime,    default =datetime.now),
)

directory_table = Table(
    'directory', metadata,
    Column('id',                Integer,        primary_key=True),
    Column('vol_id',            Integer,        ForeignKey('volume.id')),
    Column('full_path',         String,        ),
)


file_table = Table(
    'file', metadata,
    Column('id',                Integer,        primary_key=True),
    Column('vol_id',            Integer,        ForeignKey('volume.id')),
    Column('dir_id',            Integer,        ForeignKey('directory.id')),
    Column('file_name',         String(255),    ),
    Column('md5',               String(32),     nullable=False, index=True   ),
    Column('uncompressed_md5',  String(32),     index=True   ),
    Column('mtime',             DateTime,      ),
    Column('size',              Integer,       ),
)

metadata.create_all()

class Volume(object):
    def __init__(self, vol_name, notes=""):
        self.vol_name = vol_name
        self.notes = notes

class Directory(object): 
    def __init__(self, full_path, volume):
        self.full_path = full_path
        self.volume = volume
    

class File(object): 
    def __init__(self, volume, directory, filename, md5, 
                 uncompressed_md5=None, mtime=None, size=-1):
        self.volume = volume
        self.directory = directory
        self.file_name = filename
        self.md5 = md5
        if uncompressed_md5 == None:
            self.uncompressed_md5 = md5
        else:
            self.uncompressed_md5 = uncompressed_md5
        self.mtime = mtime
        self.size = size
        

mapper(Volume, volume_table)
mapper(Directory, directory_table, 
       properties=dict(volume=relation(Volume, uselist=False)))
mapper(File, file_table, 
       properties=dict(volume=relation(Volume, uselist=False),
                       directory=relation(Directory, uselist=False)))

Session = sessionmaker()
session = Session()

def get_files_by_hash(md5):
    query = session.query(File)
    query = query.filter(or_(File.md5 == md5, File.uncompressed_md5 == md5))
    return query.all()


class DbWriter(object):
    def __init__(self, volume):
        self.volume = volume
        self.session = session
        self.dir_cache = {}
        self.last_dir_path = None

    def write_file(self, full_path, md5, unc_md5, mtime=None, size=-1):
        # this is lame: full_path may contain non-ascii characters,
        # especially in the directory.  The DB can't store those in a
        # String, and I'm too lazy to figure out how to encode everything
        # into Unicode
        try:
            full_path = full_path.encode('utf-8')
        except UnicodeDecodeError:
            print "Unable to process path: %s" % full_path
            return

        dir = self.get_directory(full_path)
        file_name = os.path.basename(full_path)
        f= File(self.volume, dir, file_name, md5, unc_md5, mtime=mtime, size=size)
        self.session.save(f)

    # def done(self): flush, commit
    def get_directory(self, full_path):
        """Get a (possibly cached) Directory object for a given path."""
        dir_path = os.path.dirname(full_path)
        if dir_path in self.dir_cache:
            result = self.dir_cache[dir_path]
        else:
            # TODO: eventually, cache size should be limited, and misses
            # should involve a query on dir_path and volume
            result = Directory(dir_path, self.volume)
            self.session.save(result)
            self.dir_cache[dir_path] = result

        if dir_path != self.last_dir_path:
            # flush and commit as we move between directories
            self.last_dir_path = dir_path
            self.session.flush()
            self.session.commit()
            print '.',
            sys.stdout.flush()

        return result
