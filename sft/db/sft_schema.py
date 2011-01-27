#!/usr/bin/env python
"""
Metadata tables (sqlalchemy) for site functional tests.
"""
__author__ = "Placi Flury grid@switch.ch"
__date__ = "08.02.2010"
__version__ = "0.1.0"

import sqlalchemy as sa
from sqlalchemy.orm import mapper, relationship
from datetime import datetime
 
import sft_meta 
from sft.utils import config_parser, rsa


t_cluster = sa.Table("cluster", sft_meta.metadata,
        sa.Column("hostname", sa.types.VARCHAR(256, convert_unicode=True), primary_key=True),
        sa.Column("alias", sa.types.VARCHAR(256, convert_unicode=True), nullable=True))

t_cluster_group = sa.Table("cluster_group", sft_meta.metadata,
        sa.Column("name", sa.types.VARCHAR(256, convert_unicode=True), primary_key=True))

t_cluster_group_as = sa.Table("cluster_group_as",  sft_meta.metadata,
        sa.Column("cluster_name", None, sa.ForeignKey('cluster.hostname'), primary_key=True),
        sa.Column("group_name", None, sa.ForeignKey('cluster_group.name'), primary_key=True))

t_vo = sa.Table("vo", sft_meta.metadata,
        sa.Column("name", sa.types.VARCHAR(128, convert_unicode=True), primary_key=True),
        sa.Column("server", sa.types.VARCHAR(256, convert_unicode=True), nullable=True))

t_vo_group = sa.Table("vo_group", sft_meta.metadata,
        sa.Column("name", sa.types.VARCHAR(256, convert_unicode=True), primary_key=True))

t_vo_group_as = sa.Table("vo_group_as", sft_meta.metadata,
        sa.Column("vo_name", None, sa.ForeignKey('vo.name'), primary_key=True),
        sa.Column("group_name", None, sa.ForeignKey('vo_group.name'), primary_key=True))

t_user = sa.Table("user", sft_meta.metadata,
        sa.Column("DN", sa.types.VARCHAR(256, convert_unicode=True),primary_key=True),
        sa.Column("display_name", sa.types.VARCHAR(64, convert_unicode=True)),
        sa.Column("passwd",sa.types.Text(),nullable=False))

t_vo_user_as = sa.Table('vo_user_as', sft_meta.metadata,
        sa.Column("vo_name", None, sa.ForeignKey('vo.name'), primary_key=True),
        sa.Column("DN", None, sa.ForeignKey('user.DN'),primary_key=True))

t_test = sa.Table("test", sft_meta.metadata,
        sa.Column("name", sa.types.VARCHAR(128, convert_unicode=True), primary_key=True),
        sa.Column("xrsl", sa.types.Text(), nullable=False))

t_test_suit = sa.Table("test_suit", sft_meta.metadata,
        sa.Column("name", sa.types.VARCHAR(128, convert_unicode=True), primary_key=True))

t_test_suit_as = sa.Table("test_suit_as", sft_meta.metadata,
        sa.Column("test_name", None, sa.ForeignKey('test.name'), primary_key=True),
        sa.Column("suit_name", None, sa.ForeignKey('test_suit.name'), primary_key=True))

t_sft_test = sa.Table("sft_test", sft_meta.metadata,
        sa.Column("name", sa.types.VARCHAR(256, convert_unicode=True), primary_key=True),
        sa.Column("cluster_group", None, sa.ForeignKey('cluster_group.name')),
        sa.Column("vo_group", None, sa.ForeignKey('vo_group.name')),
        sa.Column("test_suit", None, sa.ForeignKey('test_suit.name')),
        sa.Column("minute", sa.types.VARCHAR(32, convert_unicode=True), default='0'),
        sa.Column("hour", sa.types.VARCHAR(32, convert_unicode=True), default='*'),
        sa.Column("day", sa.types.VARCHAR(32, convert_unicode=True), default='*'),
        sa.Column("month", sa.types.VARCHAR(32, convert_unicode=True), default='*'),
        sa.Column("day_of_week", sa.types.VARCHAR(32, convert_unicode=True), default='*'))


t_sft_job = sa.Table("sft_job", sft_meta.metadata,
        sa.Column('id',sa.types.Integer, primary_key=True, autoincrement=True),
        sa.Column('sft_test_name', None, sa.ForeignKey('sft_test.name')),
        sa.Column('cluster_name', None, sa.ForeignKey('cluster.hostname')),
        sa.Column('vo_name', None, sa.ForeignKey('vo.name')),
        sa.Column('test_name', None, sa.ForeignKey('test_suit.name')),
        sa.Column('DN', None, sa.ForeignKey('user.DN')),
        sa.Column('jobid', sa.types.VARCHAR(256, convert_unicode=True)),
        sa.Column('error_type', sa.types.VARCHAR(128, convert_unicode=True), default=None),
        sa.Column('error_msg', sa.types.Text()),
        sa.Column('outputdir',sa.types.VARCHAR(256, convert_unicode=True)),
        sa.Column('status', sa.types.VARCHAR(63, convert_unicode=True), default=None),
        sa.Column("submissiontime",sa.types.DateTime, default=datetime.utcnow),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow)
)

class Cluster(object):
    def __init__(self,hostname, alias=None):
        self.hostname = hostname
        self.alias = alias

class ClusterGroup(object):
    def __init__(self,name):
        self.name = name

class VO(object):
    def __init__(self,name, server=None):
        self.name = name
        self.server = server

class VOGroup(object):
    def __init__(self,name):
        self.name = name

class User(object):
    """
    User password is stored encrypted on db.
    """
    def __init__(self,DN,display_name, pwd):
        self.DN = DN
        self.display_name = display_name
        self.passwd = None
        self.reset_passwd(pwd)

    def reset_passwd(self,pwd):
        """ if a 'new_private_key' is given, we will cipher
        the pwd with this one. Used when cert is expiring.
        """
        privkey = config_parser.config.get('private_key') 
        pubkey =  config_parser.config.get('public_key')
        new_privkey = config_parser.config.get('new_private_key')
        new_pubkey =  config_parser.config.get('new_public_key')
       
        if new_privkey:
            privkey = new_privkey
            pubkey = new_pubkey
 
        rc = rsa.RSACipher(privkey, pubkey)
        if pubkey:
            self.passwd = rc.public_encrypt(pwd)
        else: 
            self.passwd = rc.priv_public_encrypt(pwd)
        
    def get_passwd(self):
        privkey = config_parser.config.get('private_key') 
        new_privkey = config_parser.config.get('new_private_key')
        if new_privkey:
            try:
                rc = rsa.RSACipher(privkey)
                plain_passwd = rc.private_decrypt(self.passwd) 
            except:
                rc = rsa.RSACipher(new_privkey) # we do not catch exception here
                plain_passwd = rc.private_decrypt(self.passwd) 
        else:
            rc = rsa.RSACipher(privkey)
            plain_passwd = rc.private_decrypt(self.passwd) 
        
        return plain_passwd
                    
class VOUsers(object):
    pass

 
class Test(object):
    def __init__(self,name, xrsl):
        self.name = name
        self.xrsl = xrsl

class TestSuit(object):
    def __init__(self,name):
        self.name = name
    
class SFTTest(object):
    pass

class SFTJob(object):
    def __init__(self, sft_test_name, 
                error_type=None, error_msg=None):
        self.sft_test_name = sft_test_name
        self.error_type = error_type
        self.error_msg = error_msg


mapper(Cluster, t_cluster)

# N:M relation between ClusterGroup and Cluster
# We do not cascade anything. -> removing group 
# will update associative table accordingly
mapper(ClusterGroup, t_cluster_group,
    properties=dict(clusters=relationship(Cluster,
    secondary=t_cluster_group_as, backref='groups'))
)


mapper(VO, t_vo)
# N':M' relationship between VOGroup and VO 
# We do not cascade anything.
mapper(VOGroup, t_vo_group,
    properties=dict(vos=relationship(VO,
    secondary=t_vo_group_as, backref='groups'))
)

mapper(User,t_user,
    properties=dict(vos=relationship(VO,
    secondary=t_vo_user_as, backref='users'))
)

mapper(Test, t_test)
# N":M" relationship between testsuit and tests
mapper(TestSuit, t_test_suit,
    properties=dict(tests=relationship(Test,
    secondary=t_test_suit_as, backref='suits'))
)

# 1:1 mappings 
mapper(SFTTest, t_sft_test)

mapper(SFTJob, t_sft_job)

