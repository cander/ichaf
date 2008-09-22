import os
from sqlalchemy import *
from sqlalchemy.orm import *
from datetime import datetime

metadata = MetaData('sqlite:///disks.db')
metadata.bind.echo = True

volume_table = Table(
    'volume', metadata,
    Column('id',                Integer,        primary_key=True),
    Column('vol_name',          String(60),        ),
    Column('notes',             String,        ),
    Column('mtime',             DateTime,    default =datetime.now),
)

file_table = Table(
    'file', metadata,
    Column('id',                Integer,        primary_key=True),
    Column('vol_id',            Integer,        ForeignKey('volume.id')),
    Column('full_path',         String,    ),
    Column('file_name',         String(255),    ),
    Column('md5',               String(32),     nullable=False   ),
    Column('uncompressed_md5',  String(32),    ),
    Column('mtime',             DateTime,    ),
)

metadata.create_all()

class File(object): 
    def __init__(self, full_path, md5, uncompressed_md5=None, mtime=None,
                 volume=None):
        self.full_path = full_path
        self.file_name = os.path.basename(full_path)
        self.md5 = md5
        if uncompressed_md5 == None:
            self.uncompressed_md5 = md5
        else:
            self.uncompressed_md5 = uncompressed_md5
        self.mtime = mtime
        self.volume = volume

class Volume(object) : pass

mapper(Volume, volume_table)
mapper(File, file_table, 
       properties=dict(volume=relation(Volume, uselist=False)))

Session = sessionmaker()
session = Session()


class DbWriter(object):
    def __init__(self, volume):
        self.volume = volume
        self.session = session

    def write_file(self, full_path, md5, mtime=None):
        #print '%s|%s|%s' % (name, full_path, md5)
        print '%s %s' % (md5, full_path)
        print '%s %s' % (md5, full_path)
        if mtime:
            mtime = datetime.fromtimestamp(mtime)
        f= File(full_path, md5, mtime=mtime)
        self.session.save(f)

    def end_dir(self):
        self.session.flush()
        self.session.commit()


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
