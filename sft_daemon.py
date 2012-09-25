#!/usr/bin/env python
"""
Site Functional Test daemon. Place in/link to init.d.

"""
__author__ = "Placi Flury grid@switch.ch"
__date__ = "13.04.2012"
__version__ = "0.3.0"

import logging
import logging.config
import sys
from optparse import OptionParser
from sqlalchemy import engine_from_config

from gridmonitor.model.nagios import init_model as init_nagios_model

import sft.sft_globals as g
from sft import init_configuration, init_nagios_notifier, init_proxy_util
from sft.daemon import Daemon
from sft.db import init_model
from sft.scheduler import Scheduler

class SFTDaemon(Daemon):
    """ Daemon for Site Functional Tests (SFTs) """

    def __init__(self, pidfile="/var/run/sft_daemon.pid"):
        self.log = logging.getLogger(__name__)
        Daemon.__init__(self, pidfile)
        self.__get_options()
        # import only after having initialized config
        init_nagios_notifier(g.config)
        init_proxy_util(g.config)

        # db connections
        try:
            sft_engine = engine_from_config(g.config.sqlalchemy_sft, 'sqlalchemy_sft.')
            init_model(sft_engine)
            self.log.info("SFT DB connection initialized")

            nagios_engine = engine_from_config(g.config.sqlalchemy_nagios, 'sqlalchemy_nagios.')
            init_nagios_model(nagios_engine)
            self.log.info('Nagios DB connection initialized')

        except Exception, e:
            self.log.error("Session object to database(s) failed: %r", e)
            sys.exit(-1)

        self.scheduler = Scheduler()
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
            init_configuration(options.config_file)
        except Exception, e:
            self.log.error("While reading configuration %s got: %r" % (options.config_file, e))
            sys.exit(-1)


    def change_state(self):
        if self.command == 'start':
            self.log.info("starting daemon...")
            daemon.start()
            self.log.info("started...")
        elif self.command == 'stop':
            self.log.info("stopping daemon...")
            self.scheduler.stop()
            daemon.stop()
            self.log.info("stopped")
        elif self.command == 'restart':
            self.log.info("restarting daemon...")
            daemon.stop()
            daemon.start()
            self.log.info("restarted")


    def run(self):

        self.scheduler.start()


if __name__ == "__main__":
    
    logging.config.fileConfig("/opt/smscg/sft/etc/logging.conf")
    #daemon = SFTDaemon(pidfile='/home/flury/sft.pid')
    daemon = SFTDaemon()
    daemon.change_state()

