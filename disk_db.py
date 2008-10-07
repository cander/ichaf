import os
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

class Volume(object) : pass
class Directory(object): 
    def __init__(self, full_path, volume=None):
        self.full_path = full_path
        self.volume = volume
    

class File(object): 
    def __init__(self, directory, filename, md5, 
                 uncompressed_md5=None, mtime=None, size=-1,
                 volume=None):
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


class DbWriter(object):
    def __init__(self, volume):
        self.volume = volume
        self.session = session
        self.dir_cache = {}
        self.last_dir_path = None

    def write_file(self, full_path, md5, unc_md5, mtime=None, size=-1):
        #print '%s|%s|%s' % (name, full_path, md5)
        print '%s %s %s' % (md5, full_path, unc_md5)
        dir = self.get_directory(full_path)
        file_name = os.path.basename(full_path)
        f= File(dir, file_name, md5, unc_md5, mtime=mtime, size=size)
        self.session.save(f)

    def get_directory(self, full_path):
        """Get a (possibly cached) Directory object for a given path."""
        dir_path = os.path.dirname(full_path)
        if dir_path in self.dir_cache:
            result = self.dir_cache[dir_path]
        else:
            # TODO: eventually, cache size should be limited, and misses
            # should involve a query on dir_path and volume
            result = Directory(dir_path)
            self.session.save(result)
            self.dir_cache[dir_path] = result

        if dir_path != self.last_dir_path:
            # flush and commit as we move between directories
            self.last_dir_path = dir_path
            self.session.flush()
            self.session.commit()

        return result


if __name__ == '__main__':

    vol_q = session.query(Volume)
    vol1 = vol_q.get(1)

    f= File('a/b/c', '1234', volume = vol1, mtime=datetime.now())
    session.save(f)
    session.flush()
    session.commit()

    files = session.query(File)
    print '###########'
    print 'files:', files.count()
