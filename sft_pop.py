#!/usr/bin/env python
"""
Populate SFT database with tests
"""
from __future__ import with_statement 
import logging, logging.config
from init import init_config, init_model
import utils.config_parser as config_parser

from db.cluster_handler import ClusterPool, ClusterGroupPool
from db.vo_handler import *
from db.test_handler import TestPool, TestSuitPool
from db.sft_handler import SFTPool
from db.user_handler import UserPool

if __name__ == "__main__":
    
    logging.config.fileConfig("./config/logging.conf")
    log = logging.getLogger(__name__)
    init_config('./config/config.ini') 

    db = config_parser.config.get('database') 
    try:
        init_model(db)
        log.info("Session object to local database created")
    except Exception, e:
        log.error("Session object to local database failed: %r", e)
    
    
    cl = ClusterPool()
    cl.add_cluster('nordugrid.unibe.ch','Bern UBELIX T3 Cluster')
    cl.add_cluster('ce.lhep.unibe.ch', 'Bern ATLAS T3')
    cl.add_cluster('smscg.epfl.ch', 'SMSCG_EPFL')
    cl.add_cluster('disir.switch.ch', 'SMSCG - SWITCH')
    cl.add_cluster('arctest.hesge.ch', 'arcXWCH at HEPIA, Geneva')
    cl.add_cluster('globus.vital-it.ch', 'SMSCG - Vital-IT')
    #cl.remove_cluster('ociknor.unizh.ch')
    cl.remove_cluster('smscg.unibe.ch')
    cl.add_cluster('ocikbnor.uzh.ch', 'OCI Grid Cluster')
    cl.add_cluster('idgc3grid01.uzh.ch', 'OCI Grid Cluster')
    cl.add_cluster('hera.wsl.ch', 'WSL Grid Cluster')
    cl.add_cluster('bacchus.switch.ch', 'SMSCG - TEST - SWITCH')
    cl.add_cluster('arc02.lcg.cscs.ch', 'Manno PHOENIX T2')

    cl_grp = ClusterGroupPool()
    cl_grp.create_group("smscg_clusters")
    cl_grp.add_cluster('smscg_clusters','disir.switch.ch') 
    cl_grp.add_cluster('smscg_clusters','bacchus.switch.ch') 
    cl_grp.add_cluster('smscg_clusters','nordugrid.unibe.ch') 
    cl_grp.add_cluster('smscg_clusters','ce.lhep.unibe.ch') 
    cl_grp.add_cluster('smscg_clusters','smscg.epfl.ch') 
    cl_grp.add_cluster('smscg_clusters','arctest.hesge.ch') 
    cl_grp.add_cluster('smscg_clusters','globus.vital-it.ch') 
    cl_grp.add_cluster('smscg_clusters','hera.wsl.ch') 
    cl_grp.add_cluster('smscg_clusters','ocikbnor.uzh.ch') 
    cl_grp.add_cluster('smscg_clusters','idgc3grid01.uzh.ch') 


    cl_grp.create_group("atlas_clusters")
    cl_grp.add_cluster('atlas_clusters','nordugrid.unibe.ch') 
    cl_grp.add_cluster('atlas_clusters','ce.lhep.unibe.ch') 

    cl_grp.create_group("life_clusters")
    cl_grp.add_cluster('life_clusters','globus.vital-it.ch') 
    cl_grp.add_cluster('life_clusters','hera.wsl.ch') 
    cl_grp.add_cluster('life_clusters','ocikbnor.uzh.ch') 
    cl_grp.add_cluster('life_clusters','idgc3grid01.uzh.ch') 

    vo = VOPool()
    vo.add_vo('life','voms.smscg.ch')
    vo.add_vo('smscg','voms.smscg.ch')
    vo.add_vo('crypto','voms.smscg.ch')
    vo.add_vo('tutor','voms.smscg.ch')
    vo.add_vo('earth','voms.smscg.ch')
    vo.add_vo('atlas','voms.cern.ch')
   
     
    vo_grp = VOGroupPool()
    vo_grp.create_group('noRTEpool')
    vo_grp.add_vo('noRTEpool','smscg')
    vo_grp.add_vo('noRTEpool','crypto')
    vo_grp.add_vo('noRTEpool','tutor')
    
    vo_grp.create_group('lifepool')
    vo_grp.add_vo('lifepool','life')
    
    vo_grp.create_group('earthpool')
    vo_grp.add_vo('earthpool','earth')


    up = UserPool()
    pf = '/DC=ch/DC=switch/DC=slcs/O=Switch - Teleinformatikdienste fuer Lehre und Forschung/CN=Placi Flury C82EEB1A'
    pf_pwd = 'lap5ns'
    up.remove_user(pf)
    up.add_user(pf,pf_pwd)

    sergio ='/DC=ch/DC=switch/DC=slcs/O=Universitaet Zuerich/CN=Sergio Maffioletti FD0DDA88'
    sergio_pwd = 'fak3'
    up.add_user(sergio,sergio_pwd)

    ale='/DC=ch/DC=switch/DC=slcs/O=Switch - Teleinformatikdienste fuer Lehre und Forschung/CN=Alessandro Usai 5B9F01EF'
    ale_pwd = 'r0ckst4r'
    up.add_user(ale,ale_pwd)

    vo_upool = VOUserPool()
    vo_upool.add_user('smscg',pf)
    vo_upool.add_user('smscg',sergio)
    vo_upool.add_user('smscg',ale)

    vo_upool.add_user('crypto',pf)
    vo_upool.add_user('crypto',sergio)
    vo_upool.add_user('life',sergio)


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
 
