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

class File(object) : pass
class Volume(object) : pass

mapper(Volume, volume_table)
mapper(File, file_table)

Session = sessionmaker()
session = Session()

vol_q = session.query(Volume)
vol1 = vol_q.get(1)

f= File()
f.vol_id = vol1
f.full_path = 'a/b/c'
f.file_name = 'c'
f.md5 = '1234'
session.save(f)
session.flush()
session.commit()

files = session.query(File)
print '###########'
print 'files:', files.count()
