#!/usr/bin/env python
# -*- coding: utf-8  -*-

"""
Populate SFT database with tests
"""
from __future__ import with_statement 
import logging, logging.config
from sqlalchemy import engine_from_config

from sft.utils import init_config
import sft.utils.config_parser as config_parser
from sft.db import init_model
from sft.db.cluster_handler import ClusterPool, ClusterGroupPool
from sft.db.vo_handler import *
from sft.db.test_handler import TestPool, TestSuitPool
from sft.db.sft_handler import SFTPool
from sft.db.user_handler import UserPool

if __name__ == "__main__":
    
    logging.config.fileConfig("./logging.conf")
    log = logging.getLogger(__name__)
    init_config('./config.ini') 

    try:
        sft_engine = engine_from_config(config_parser.config.get(), 'sqlalchemy_sft.')
        init_model(sft_engine)
        log.info("Session object to local database created")
    except Exception, e:
        log.error("Session object to local database failed: %r", e)
    
    
    cl = ClusterPool()
    cl.add_cluster('bacchus.switch.ch', 'SMSCG - TEST - SWITCH')
    cl.add_cluster('alemonia.switch.ch', 'SMSCG - T2')

    cl_grp = ClusterGroupPool()
    cl_grp.create_group("smscg_clusters")
    cl_grp.add_cluster('smscg_clusters','alemnia.switch.ch') 
    cl_grp.add_cluster('smscg_clusters','bacchus.switch.ch') 

    vo = VOPool()
    vo.add_vo('smscg','voms.smscg.ch')
    vo.add_vo('atlas','voms.cern.ch')
   
     
    vo_grp = VOGroupPool()
    vo_grp.create_group('noRTEpool')
    vo_grp.add_vo('noRTEpool','smscg')
    vo_grp.add_vo('noRTEpool','atlas')
    

    up = UserPool()
    pf = '/DC=ch/DC=switch/DC=slcs/O=Switch - Teleinformatikdienste fuer Lehre und Forschung/CN=Placi Flury C82EEB1A'
    pf_pwd = 'secret1'
    up.remove_user(pf)
    up.add_user(pf,'Placi Flury', pf_pwd)
    
    vo_upool = VOUserPool()
    vo_upool.add_user('smscg',pf)
    vo_upool.add_user('atlas',pf)

    tst = TestPool()
    xrsl = ""
    with open("./test/job1.xrsl") as f:
        for line in f.readlines():
            xrsl += line
    tst.add_test('test1',xrsl)
    
    xrsl2 = ""
    with open("./test/job2.xrsl") as f:
        for line in f.readlines():
            xrsl2 += line
    tst.add_test('test2',xrsl2)
   
    smscg_sft01_xrsl = ""
    with open("./test/smscg_sft01.xrsl") as f:
        for line in f.readlines():
            smscg_sft01_xrsl += line
    tst.add_test('smscg_sft1', smscg_sft01_xrsl)
    
    smscg_sft02_xrsl = ""
    with open("./test/smscg_sft02.xrsl") as f:
        for line in f.readlines():
            smscg_sft02_xrsl += line
    tst.add_test('smscg_sft2', smscg_sft02_xrsl)


 
    tst_suit = TestSuitPool()
    tst_suit.create_suit('arc_std_suit')
    tst_suit.add_test('arc_std_suit','test1') 
    tst_suit.add_test('arc_std_suit','test2') 
   
    tst_suit.create_suit('smscg_std_suit')
    tst_suit.add_test('smscg_std_suit','smscg_sft1') 
    tst_suit.add_test('smscg_std_suit','smscg_sft2') 
    
    sft = SFTPool()
    sft.add_sft('arc_std_sft','smscg_clusters', 'noRTEpool','arc_std_suit')
    sft.set_exectime('arc_std_sft', minute='1', hour='2', day='1', month= '*/1')
    sft.add_sft('smscg_std_sft','smscg_clusters', 'noRTEpool','smscg_std_suit')
    sft.set_exectime('smscg_std_sft', minute='23', hour='9', day='*/1', month= '*/1')
 
