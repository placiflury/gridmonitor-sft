"""
Dealing with SFT tests.
"""

import logging
import sft_meta
import sft_schema as schema
from sft.utils.helpers import strip_args

class SFTPool():
    """ This class defines all site functional tests (SFT)s 
        that shall be executed.
    """
    
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.session=sft_meta.Session()
        self.log.debug("Initialization finished")
    
    def __del__(self):
        self.session.close()

    @strip_args
    def add_sft(self, name, cluster_grp, vo_grp,test_suit):
        """ Adding a new SFT to the 'global' pool of SFTS.
            params: name - the name of the SFT, must be unique
                    cluster_grp - the name of the cluster group (see ClusterGroupPool) 
                                to which SFTs shall apply
                    vo_grp      - the name of the VO group (see VOGroupPool), to 
                                which SFT shall apply
                    test_suit   - the suit of tests the SFT consists of
            Notice: the execution time, is must be set via the set_exectime method
            Notice: XXX checks whether cluster_grp, vo_grp and test_suit exist are currently missing
        """

        sft = self.session.query(schema.SFTTest).filter_by(name=name).first()
        if sft:
            self.log.info("SFT test '%s' exists already, overwriting" % name)
        else:
            self.log.debug("Adding SFT '%s'." % name)
            sft = schema.SFTTest()
            sft.name = name
        sft.cluster_group = cluster_grp
        sft.vo_group = vo_grp
        sft.test_suit = test_suit
        self.session.add(sft)
        self.session.commit() 

    @strip_args
    def set_exectime(self, name, minute='0', hour='*',
                    day='*', month='*', weekday='*'):
        """ Setting execution time of the SFT.
            params: name - name of the SFT
                    minute - minute 0-59, default 0
                    hour    - hour  0-23, default * 
                    day     - day   1-31, default *
                    month   - month 1-12, default * 
                    weekday - day of week 0-6, Sunday=0, default * 
            Notice: for each param, you can use crontab notation, e.g. '*', '1-3', '*/5', etc.
        """
        sft = self.session.query(schema.SFTTest).filter_by(name=name).first()
        if sft:
            sft.minute = minute
            sft.hour = hour
            sft.day = day
            sft.month= month
            sft.weekday = weekday
            self.session.commit()
        
    @strip_args
    def remove_sft(self, name):
        """ removing SFT from SFT pool.
            params: name - name of SFT to remove
        """
        sft = self.session.query(schema.SFTTest).filter_by(name=name).first()
        if sft:
            self.log.debug("Removing sft '%s'." % name)
            self.session.delete(sft)   
            self.session.commit()

    def list_sfts(self):
        """ Listing of all existing SFTs in pool.
            returns list of SFT objects
        """
        return self.session.query(schema.SFTTest).filter_by(name=name).all()
    

