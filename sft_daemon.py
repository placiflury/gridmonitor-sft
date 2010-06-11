#!/usr/bin/env python
"""
reads sft dababase and checks whether something needs to be executed.

XXX: monitor it with Nagios
"""
__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "19.02.2010"
__version__ = "0.1.0"

import logging, logging.config
import sys, time 
from optparse import OptionParser
from datetime import datetime, timedelta
from sqlalchemy import orm

import sft.utils.config_parser as config_parser
from sft.daemon import Daemon
from sft.utils import init_config
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
            init_model(self.db)
            self.log.info("Session object to local database created")
        except Exception, e:
            self.log.error("Session object to local database failed: %r", e)

        Session = orm.scoped_session(meta.Session)
        self.session = Session()
        self.reset_sft_events() # read SFT events
        self.publisher = Publisher(self.jobsdir, self.url_root)
        self.cleaner = Cleanex(self.max_jobs_age, self.url_root)
        self.log.debug("Initialization finished")

    def __get_options(self):
        usage = "usage: %prog [options] start|stop|restart \n\nDo %prog -h for more help."
        parser = OptionParser(usage=usage, version ="%prog " + __version__)
        parser.add_option("" , "--config_file", action="store",
            dest="config_file", type="string",
            default="config/config.ini",
            help="File holding the sft specific configuration for this site (default=%default)")

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
        
        self.db = config_parser.config.get('database')
        if not self.db:
            self.log.error("'database' option missing in %s." % (options.config_file))
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

        self.ngsub_path = config_parser.config.get('ngsub_path')

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

    def _parse_cron(self, str, max):
        """
        parsing *simple* crontab style entries. 
        covered cases:

         - integer up to max
         - lists  '1,2,3' 
         - ranges  '1-3'
         - steps   '*/4' or '1-4/2'
         - mix list and range '1,2,3,8-12'

        XXX improve (currently no error handling, input checking  etc.)
        """
        if str.isdigit():
            v = int(str)
            if v > max:
                return max
            return v

        if '/' in str: # step
            pre = str.split('/')[0]
            step = int(str.split('/')[1])
            if pre == '*':
                return range(0, max+1, step)

            if '-' in pre:
                start = int(pre.split('-')[0])        
                end = int(pre.split('-')[1])
                if end > max:
                    end = max
                return range(start, end+1, step)
           
        if ('-' in str) and (',' in str):
            res = list()
            for i in str.split(','):
                v = self._parse_cron(i, max) 
                if type(v) is int:
                    res.append(v)
                else:
                    res += v
            if res:
                return res
 
        if ',' in str:
            res = list()
            for i in str.split(','):
                if i.isdigit():
                    v = int(i)
                    if v > max:
                        res.append(max)
                    else: 
                        res.append(v)     
            return res
        
        if '-' in str:
            start = int(str.split('-')[0])        
            end = int(str.split('-')[1])
            if end > max:
                end = max
            return range(start, end+1, 1)


    def reset_sft_events(self):
        self.sft_events = list()
        for sft in self.session.query(schema.SFTTest).all():
            minute = self._parse_cron(sft.minute, 59)
            hour = self._parse_cron(sft.hour, 23)
            day = self._parse_cron(sft.day, 31)
            month = self._parse_cron(sft.month, 12)
            dow = self._parse_cron(sft.day_of_week, 7)

            event = SFT_Event(sft.name,
                    minute = minute, hour = hour,
                    day = day, month = month,
                    dow = dow)
            event.set_ngsub_path(self.ngsub_path)
            self.sft_events.append(event)


    def run(self):
        t = datetime(*datetime.now().timetuple()[:5])
        
        check_counter = SFTDaemon.CHECK_JOBS_EVERY_MINUTES  

        while True:
            for e in self.sft_events:
                self.log.debug("Checking SFT event '%s'" % e.get_name())
                e.check_exec(t)

            t += timedelta(minutes=1)
            n = datetime.now()
            while n < t:
                s = (t - n).seconds # makes sure we do not get t < datetime.now() before going to sleep.
                time.sleep(s)
                n = datetime.now()
            
            if check_counter <= 0:
                self.publisher.main()
                self.cleaner.check_jobs()
                check_counter = SFTDaemon.CHECK_JOBS_EVERY_MINUTES 
                continue
            check_counter -= 1            
            

if __name__ == "__main__":
    
    logging.config.fileConfig("/opt/smscg/sft/etc/logging.conf")
    #daemon = SFTDaemon(pidfile='/home/flury/sft.pid')
    daemon = SFTDaemon()
    daemon.change_state()

