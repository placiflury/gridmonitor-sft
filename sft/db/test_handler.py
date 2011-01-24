"""
Dealing with test and test suits
"""

import logging
import sft_meta
import sft_schema as schema
from sft.utils.helpers import strip_args

class TestPool():
    """ The test pool class is used to define a global set 
        of tests, which can later be used by the 'TestSuitPool' 
        instance to built test-suits.
    """
    
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.session=sft_meta.Session()
        self.log.debug("Initialization finished")

    @strip_args
    def add_test(self, name, xrsl):
        """ Adds a test to the 'global' pool of tests.
            params: name - name of the test, must be unique
                    xrsl - an XRSL description of the test
        """
        test = self.session.query(schema.Test).filter_by(name=name).first()
        if test:
            self.log.info("Test '%s' exists already, updating" % name)
            if test.xrsl != xrsl:
                test.xsl = xrsl
        else:
            self.log.debug("Adding test '%s'." % name)
            test = schema.Test(name,xrsl)

        self.session.add(test)
        self.session.commit() 

    @strip_args
    def remove_vo(self, name):
        """ Removing a test from the pool of tests.
            params: name - name of the test to remove
        """
        test = self.session.query(schema.Test).filter_by(name=name).first()
        if vo:
            self.log.debug("Removing test '%s'." % name)
            self.session.delete(test)   
            self.session.commit()

    def list_tests(self):
        """ Listing of the currently existing tests.
            Return lists of test objects.
         """
        return self.session.query(schema.Test).filter_by(name=name).all()

    

class TestSuitPool():
    """ Class which allows to create a pool of test-suits. SFTs can later
        be built by specifying the test suits they should consist of.
     """
        
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.session=sft_meta.Session()
        self.log.debug("Initialization finished")

    @strip_args
    def create_suit(self, suitname):
        """ Creating a new test suit in the pool of test suits""
            params: suitname - name of the test suite, must be unique.
        """
        suit = self.session.query(schema.TestSuit).filter_by(name=suitname).first()

        if suit:
            self.log.info("Test suit '%s' exists already" % suitname)
        else:
            self.session.add(schema.TestSuit(suitname))
            self.session.commit()


    @strip_args
    def remove_suit(self, suitname):
        """ Removing a test suit from the 'global' pool of test suits.
            params: suitname - name of the test suit to remove
        """
        suit = self.session.query(schema.TestSuit).filter_by(name=suitname).first()
        if suit:
            self.log.debug("Removing suit '%s'." % suitname)
            self.session.delete(suit)
            self.session.commit()
             

    @strip_args
    def add_test(self, suitname, testname):
        """ Adding a test to a test suit. If the test suit does not yet exist
            it will be created on the fly.
            params: suitname - name of the test suit to which test should be added
                    testnaem - name of the test to add to the test suit.
        """
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
            self.log.info("Added test '%s' to test suit '%s'" % (testname, suitname))
            suit.tests.append(test) 
        
        self.session.commit()
        
    def list_testsuits(self):
        """ Listing all test suits that have been defined so 
            far in the pool of test suits.
            return: list of test suit objects
        """
        return self.session.query(schema.TestSuit).filter_by(name=suitname).all()
    
    @strip_args
    def list_tests(self, testsuit):
        """ Listing all test belonging to testsuit.
            return: list of test objects, or None (e.g. if testsuit does not exist)
        """
        suit = self.session.query(schema.TestSuit).filter_by(name=suitname).first()
        if not suit or not suit.tests:
            return None

        return suit.tests
