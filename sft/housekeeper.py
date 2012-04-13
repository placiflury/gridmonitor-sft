"""
Housekeeping module with the following chores:
    . keeping SFTs up-to-date, e.g. execution times, 
      number of SFTs
    . keep track of sites (clusters) with scheduled downtimes.
    . clean up of old SFT job results
"""
import logging
import time
import shutil
from datetime import datetime

from gridmonitor.model.nagios import meta as nagios_meta
from gridmonitor.model.nagios import scheduleddowntimes

import db.sft_meta as meta
import db.sft_schema as schema

from utils import helpers
from nagios_notifier import NagiosNotification

import sft_globals as g
from sft_event import SFT_Event

class Housekeeper(object):
    
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.sft_list = []
        self.down_clusters = []
        self.sft_refresh_cnt = g.config.refresh_period # refreshing SFT period 
                            # using (same counter for cleaning up old jobs)
        self.__refresh_down_clusters()
        self.__refresh_SFTs()
    
    def __refresh_down_clusters(self):
        """ refreshes list of clusters with scheduled downtime. """

        self.down_clusters = []
        now = datetime.now() # not UTC as nagios is using local time

        for sched_item in nagios_meta.Session().\
                query(scheduleddowntimes.ScheduledDownTime).all():
            if sched_item.scheduled_start_time > now:
                continue
            if sched_item.scheduled_end_time < now:
                continue
            _host = sched_item.generic_object.name1
            if _host not in self.down_clusters:
                self.down_clusters.append(_host)

        self.log.info("Hosts currently scheduled down: %r" % self.down_clusters)

    def __refresh_SFTs(self):
        """
            refreshes list of SFTs and the 
            internals of the SFTs (e.g. like 
            changed execution times etc.)
        """
        self.sft_list = list()
        
        for sft in helpers.get_sft_tests_details():
            self.log.debug("Refreshing SFT '%s' from db" % sft.name)
            try: 
                _minute = helpers.parse_cron_entry(sft.minute, 59)
                _hour = helpers.parse_cron_entry(sft.hour, 23)
                _day = helpers.parse_cron_entry(sft.day, 31)
                _month = helpers.parse_cron_entry(sft.month, 12)
                _dow = helpers.parse_cron_entry(sft.day_of_week, 6)

                event = SFT_Event(sft.name,
                        minute = _minute, hour = _hour,
                        day = _day, month = _month,
                        dow = _dow)
            except Exception, ex:
                _notification = NagiosNotification(g.config.localhost, sft.name)
                _notification.set_message(str(ex)) # due to depreciation warning
                _notification.set_status('CRITICAL')
                g.notifier.add_notification(_notification)
                continue 

            self.sft_list.append(event)    


    def __clean_jobs(self):
        """ 
        removes old SFT jobs.   
        """ 
        self.log.info("XXX")
        session = meta.Session
        self.log.info("Xgot session")
        fetched_before = datetime.utcfromtimestamp(time.time() - (g.config.max_jobs_age * 60))

        fjobs = session.query(schema.SFTJob).filter(schema.SFTJob.db_lastmodified<=fetched_before).all()

        self.log.info("Xfetched jobs")
        if fjobs:
            self.log.info("Removing %d jobs from db that got fetched." % len(fjobs))
            for j in fjobs:
                if j.outputdir:
                    jobdir = g.config.url_root + j.outputdir
                    self.log.info("Removing job directory at '%s'" % jobdir)
                    try:
                        shutil.rmtree(jobdir)
                    except:
                        pass
                session.delete(j)
            session.commit()

    @property
    def sfts(self):
        """     
        returns list of currently defined SFT (objects)
        """
        return self.sft_list

    @property
    def scheduled_down_clusters(self):
        """ 
            returns list of sites, respectively of
            the front-ends (aka clusters) that are
            on scheduled downtime.
        """
        return self.down_clusters

    def main(self):
        
        self.__refresh_down_clusters()

        self.sft_refresh_cnt -= 1
        if self.sft_refresh_cnt <= 0:
            self.__refresh_SFTs()
            self.__clean_jobs()
            self.sft_refresh_cnt = g.config.refresh_period

