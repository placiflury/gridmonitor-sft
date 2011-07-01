#!/usr/bin/env python
"""
reads sft dababase and checks whether something needs to be executed.

"""
__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "19.02.2010"
__version__ = "0.1.0"

import logging
import logging.config
import sys
import time 
from optparse import OptionParser
from datetime import datetime, timedelta
from sqlalchemy import engine_from_config

# using gridmontior model to get access to nagios ndoutils db
from gridmonitor.model.nagios import init_model as init_nagios_model
from gridmonitor.model.nagios import meta as nagios_meta
from gridmonitor.model.nagios import scheduleddowntimes

import sft.utils.config_parser as config_parser
from sft.daemon import Daemon
from sft.utils import init_config
from sft.utils.helpers import * 
from sft.db import init_model
import sft.db.sft_meta as meta
import sft.db.sft_schema as schema
from sft.sft_event import SFT_Event
from sft.publisher import Publisher
from sft.dbcleaner import Cleanex



class SFTDaemon(Daemon):
    """ Daemon for Site Functional Tests (SFTs) """

    CHECK_JOBS_EVERY_MINUTES = 20   # check sft jobs every x minutes

    def __init__(self, pidfile="/var/run/sft_daemon.pid"):
        self.log = logging.getLogger(__name__)
        Daemon.__init__(self, pidfile)
        self.__get_options()
        try:
            sft_engine = engine_from_config(config_parser.config.get(), 'sqlalchemy_sft.')
            init_model(sft_engine)
            self.log.info("SFT DB connection initialized")

            nagios_engine = engine_from_config(config_parser.config.get(), 'sqlalchemy_nagios.')
            init_nagios_model(nagios_engine)
            self.log.info('Nagios DB connection initialized')

        except Exception, e:
            self.log.error("Session object to local database failed: %r", e)

        self.refresh_sft_events() # read SFT events
        self.publisher = Publisher(self.jobsdir, self.url_root)
        self.publisher.set_ngstat_ngget_path(self.ng_commands_path)
        self.cleaner = Cleanex(self.max_jobs_age, self.url_root)
        self.log.debug("Initialization finished")

    def __get_options(self):
        usage = "usage: %prog [options] start|stop|restart \n\nDo %prog -h for more help."
        parser = OptionParser(usage=usage, version ="%prog " + __version__)
        parser.add_option("" , "--config_file", action="store",
            dest = "config_file", type="string",
            default = "/opt/smscg/sft/etc/config.ini",
            help = "File holding the sft specific configuration for this site (default=%default)")

        (options, args) = parser.parse_args()
        self.log.debug("Invocation with args: %r and options: %r" % (args, options))

        self.options = options

        if (not args):
            parser.error("Argument is missing.")

        if (args[0] not in ('start', 'stop', 'restart')):
            parser.error("Uknown argument")
        self.command = args[0]

        try:
            init_config(options.config_file)
        except Exception, e:
            self.log.error("While reading configuration %s got: %r" % (options.config_file, e))
            sys.exit(-1)

        self.jobsdir = config_parser.config.get('jobsdir')
        if not self.jobsdir:
            self.log.error("'jobsdir' option missing in %s." % (options.config_file))
            sys.exit(-1)
        
        self.url_root = config_parser.config.get('url_root')
        if not self.url_root:
            self.log.error("'url_root' option missing in %s." % (options.config_file))
            sys.exit(-1)
            
        max_jobs_age = config_parser.config.get('max_jobs_age')
        if not max_jobs_age:
            self.log.info("'max_jobs_aget' option missing in %s, setting it to 86400 seconds." \
                % (options.config_file))
            max_jobs_age = 86400  # 1 day
        self.max_jobs_age = int(max_jobs_age)

        self.ng_commands_path = config_parser.config.get('ng_commands_path')
        self.sft_refresh_period = int(config_parser.config.get('refresh_period'))

    def change_state(self):
        if self.command == 'start':
            self.log.info("starting daemon...")
            daemon.start()
            self.log.info("started...")
        elif self.command == 'stop':
            self.log.info("stopping daemon...")
            daemon.stop()
            self.log.info("stopped")
        elif self.command == 'restart':
            self.log.info("restarting daemon...")
            daemon.restart()
            self.log.info("restarted")


    def refresh_sft_events(self):
        # we expect that the cron-job input(s) has been 
        # checked before already! thus no error handling -> XXX be conservative? 
        self.sft_events = list()
        session = meta.Session()
        
        down_clusters = self.get_downtime_hosts()


        for sft in session.query(schema.SFTTest).all():
            _minute = parse_cron_entry(sft.minute, 59)
            _hour = parse_cron_entry(sft.hour, 23)
            _day = parse_cron_entry(sft.day, 31)
            _month = parse_cron_entry(sft.month, 12)
            _dow = parse_cron_entry(sft.day_of_week, 6)

            event = SFT_Event(sft.name,
                    minute = _minute, hour = _hour,
                    day = _day, month = _month,
                    dow = _dow)
            event.set_ngsub_path(self.ng_commands_path)
            event.set_clusters_down(down_clusters)
            self.sft_events.append(event)
            

    def get_downtime_hosts(self):
        """ Getting list of hosts with current downtime
            from nagios (ndoutils) database. 
        """
        hosts = list()
        
        now = datetime.now() # not UTC as nagios is using local time

        for sched_item in nagios_meta.Session().query(scheduleddowntimes.ScheduledDownTime).all():
            
            if sched_item.scheduled_start_time > now:
                continue
            if sched_item.scheduled_end_time < now:
                continue
            
            _host = sched_item.generic_object.name1
            if _host not in hosts:
                hosts.append(_host)


        self.log.debug("Hosts currently scheduled down: %r" % hosts)
        return hosts


    def run(self):
        t = datetime(*datetime.utcnow().timetuple()[:5])
        
        _jb_check_cnt = SFTDaemon.CHECK_JOBS_EVERY_MINUTES  
        _sft_refresh_cnt = self.sft_refresh_period

        while True:
            if _sft_refresh_cnt <= 0:
                self.log.debug("refreshing list of SFTs")
                self.refresh_sft_events()
                _sft_refresh_cnt = self.sft_refresh_period

            for e in self.sft_events:
                self.log.debug("Checking SFT event '%s'" % e.get_name())
                e.check_exec(t)

            t += timedelta(minutes=1)
            n = datetime.utcnow()
            while n < t:
                s = (t - n).seconds # makes sure we do not get t < datetime.now() before going to sleep.
                time.sleep(s)
                n = datetime.utcnow()
            
            if _jb_check_cnt <= 0:
                self.publisher.main()
                self.cleaner.check_jobs()
                _jb_check_cnt = SFTDaemon.CHECK_JOBS_EVERY_MINUTES 
                continue

            _jb_check_cnt -= 1            
            _sft_refresh_cnt -= 1
            

if __name__ == "__main__":
    
    logging.config.fileConfig("/opt/smscg/sft/etc/logging.conf")
    #daemon = SFTDaemon(pidfile='/home/flury/sft.pid')
    daemon = SFTDaemon()
    daemon.change_state()

