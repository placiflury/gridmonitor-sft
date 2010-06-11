"""
Dealing with test and test suits
"""

import logging
import sft_meta
import sft_schema as schema
from sqlalchemy import orm

class TestPool():
    
    def __init__(self):
        self.log = logging.getLogger(__name__)
        Session=orm.scoped_session(sft_meta.Session)
        self.session=Session()
        self.log.debug("Initialization finished")


    def add_test(self, name, xrsl):
        test = self.session.query(schema.Test).filter_by(name=name).first()
        if test:
            self.log.info("Test '%s' exists already" % name)
            if test.xrsl != xrsl:
                test.xsl = xrsl
                self.session.flush()
        else:
            self.log.debug("Adding test '%s'." % name)
            test = schema.Test(name,xrsl)
            self.session.add(test)
            self.session.flush()
        self.session.commit() 
        #self.session.clear() # -> make sure things get reloaded freshly

    def remove_vo(self, name):
        test = self.session.query(schema.Test).filter_by(name=name).first()
        if vo:
            self.log.debug("Removing test '%s'." % name)
            self.session.delete(test)   
            self.session.flush()
            self.session.commit()
            #self.session.clear() # -> make sure things get reloaded freshly
    

class TestSuitPool():
        
    def __init__(self):
        self.log = logging.getLogger(__name__)
        Session = orm.scoped_session(sft_meta.Session)
        self.session = Session()
        self.log.debug("Initialization finished")

    def create_suit(self, suitname):
        suit = self.session.query(schema.TestSuit).filter_by(name=suitname).first()

        if suit:
            self.log.info("Test suit '%s' exists already" % suitname)
        else:
            self.session.add(schema.TestSuit(suitname))
            self.session.flush()
            self.session.commit()
            #self.session.clear() # -> make sure things get reloaded freshly


    def remove_suit(self, suitname):
        suit = self.session.query(schema.TestSuit).filter_by(name=suitname).first()
        if suit:
            self.log.debug("Removing suit '%s'." % suitname)
            self.session.delete(suit)
            self.session.flush()
            self.session.commit()
            #self.session.clear() # -> make sure things get reloaded freshly
             

    def add_test(self,suitname,testname):
        """ will create suit if it doesn't exist. """
        suit = self.session.query(schema.TestSuit).filter_by(name=suitname).first()
        test = self.session.query(schema.Test).filter_by(name=testname).first()
        
        if not test:
            self.log.warn("Test '%s' does not exist. Can't add it to group '%s'." % (testname, suitname))
            return

        if not suit:
            self.log.debug("Test suit '%s' does not exist, will be created." % suitname)
            suit = schema.TestSuit(suitname)
            self.session.add(suit)
        
        if not test in suit.tests:
            suit.tests.append(test) 
        
        self.session.flush()
        self.session.commit()
        #self.session.clear() # -> make sure things get reloaded freshly
        
