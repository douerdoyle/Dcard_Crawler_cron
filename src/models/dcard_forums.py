from datetime             import datetime
from sqlalchemy           import Column, String, DateTime, Text, text, Integer
from settings.environment import db
from sqlalchemy.dialects.mysql import TINYINT

class DcardForums(db.Model):
    __tablename__    = 'dcard_forums'
    id               = Column('id'        , String(36), primary_key=True)
    name             = Column('name'      , Text)
    alias            = Column('alias'     , Text)
    pc_l30d          = Column('pc_l30d'   , Integer) # postCount last30Days
    backtrack        = Column('backtrack' , TINYINT)
    enable           = Column('enable'    , TINYINT)
    createdAt        = Column('createdAt' , DateTime)
    updatedAt        = Column('updatedAt' , DateTime)
    ac_time          = Column('ac_time'   , DateTime)
    ac_status        = Column('ac_status' , TINYINT, default=0)
    cc_time          = Column('cc_time'   , DateTime)
    cc_status        = Column('cc_status' , TINYINT, default=0)
    data_create_time = Column('data_create_time', DateTime, server_default=text('CURRENT_TIMESTAMP'))
    data_update_time = Column('data_update_time', DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    exist            = Column('exist'     , TINYINT)

    def __init__(self, id, name, alias, pc_l30d, backtrack, createdAt, updatedAt, enable=1, exist=1, *args, **kwargs):
        self.id          = id
        self.name        = name
        self.alias       = alias
        self.pc_l30d     = pc_l30d
        self.backtrack   = backtrack
        self.exist       = exist
        self.enable      = enable
        self.ac_status   = 0
        self.cc_status   = 0
        self.createdAt   = datetime.strptime(createdAt, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
        self.updatedAt   = datetime.strptime(updatedAt, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')