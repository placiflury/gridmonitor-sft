"""
Cleans up old sft job records in db
"""
import logging, time, shutil
from sqlalchemy import orm
from datetime import datetime

import db.sft_meta as meta
import db.sft_schema as schema



__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "08.01.2010"
__version__ = "0.1.0"

class Cleanex(object):
    """ Keeping DB and Jobs download directory clean. 
        Age of records to be passed in seconds """

    def __init__(self, age, url_root):
        """ SFT job records that have not been changed 
            for 'age' [seconds] will be removed. 
            url_root: filesystem location of URL root.
        """
        self.log = logging.getLogger(__name__)
  
        self.age = age
        self.log.info("Maximal age of job records set to: %d [sec]" % age)
        self.url_root = url_root
        Session = orm.scoped_session(meta.Session)
        self.session = Session()
        self.log.debug("Initialization finished")
   
    
    def check_jobs(self):
        """ Checking for SFT jobs that can be removed. """
        self.log.debug("Checking for expired SFT job records")
 
        fetched_before = datetime.utcfromtimestamp(time.time() - self.age)
        
        fjobs = self.session.query(schema.SFTJob).filter(schema.SFTJob.db_lastmodified<=fetched_before).all()

        if fjobs:
            self.log.info("Removing %d jobs from db that got fetched." % len(fjobs)) 
            for j in fjobs:
                if j.outputdir:
                    jobdir = self.url_root + j.outputdir  
                    self.log.info("Removing job directory at '%s'" % jobdir)
                    try:
                        shutil.rmtree(jobdir)
                    except:
                        pass
                self.session.delete(j)
            self.session.flush()
            self.session.commit()
        

        
    
