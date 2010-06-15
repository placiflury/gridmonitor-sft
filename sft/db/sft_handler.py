"""
Dealing with SFT tests.
"""

import logging
import sft_meta
import sft_schema as schema
from sqlalchemy import orm

class SFTPool():
    
    def __init__(self):
        self.log = logging.getLogger(__name__)
        Session=orm.scoped_session(sft_meta.Session)
        self.session = Session()
        self.log.debug("Initialization finished")
    
    def __del__(self):
        self.session.close()

    def add_sft(self,name,cluster_grp, vo_grp,test_suit):
        """ execution times must be set by self.set_exectime"""
        # XXX check wehter cluster_grp, vo_grp and test_suit do exist
        sft = self.session.query(schema.SFTTest).filter_by(name=name).first()
        if sft:
            self.log.info("SFT test '%s' exists already" % name)
            #XXX update fields that might have changed
        else:
            self.log.debug("Adding SFT '%s'." % name)
            sft = schema.SFTTest()
            sft.name = name
            sft.cluster_group = cluster_grp
            sft.vo_group = vo_grp
            sft.test_suit = test_suit
            self.session.add(sft)
        self.session.commit() 


    def set_exectime(self, name, minute='0', hour='*',
                    day='*', month='*', weekday='*'):
        sft = self.session.query(schema.SFTTest).filter_by(name=name).first()
        if sft:
            sft.minute = minute
            sft.hour = hour
            sft.day = day
            sft.month= month
            sft.weekday = weekday
            self.session.commit()
        

    def remove_sft(self,name):
        sft = self.session.query(schema.SFTTest).filter_by(name=name).first()
        if sft:
            self.log.debug("Removing sft '%s'." % name)
            self.session.delete(sft)   
            self.session.commit()
    

