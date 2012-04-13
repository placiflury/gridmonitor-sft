"""
Nagios notification module. Uses the Nagios nsca protocol to 
push SFT notifications to a nagios server. The messages are
received there as passive tests.

Implies that the nagios nsca client package is installed on the
machine on which this program is run. 
E.g. for debian systems the nsca package is: nsca
"""
__author__ = "Placi Flury grid@switch.ch"
__date__ = "16.02.2012"
__version__ = "0.2.0"

import os
import logging
import Queue
from subprocess import Popen, PIPE
from errors.nagios import NagiosNotifierError


class NagiosNotification(object):
    """ Notification Object for Nagios """
    
    STATUS = {'OK': 0, 'WARNING': 1, 'CRITICAL': 2, 'UNKNOWN': -1}
    
    def __init__(self, host, service ):
        """ 
            host - host attributed to nofication
            service - service attributed to notification 
        """
        self.host = host
        self.service = service
        self.message = None
        self.perf_data = None
        self.status = 'UNKNOWN'
        
    def get_host(self):
        """ returns host attributed to notication"""
        return self.host

    def get_service(self):
        """ returns service attributed to notification """
        return self.service

    def set_message(self, msg):
        """ Sets (status) message of notification. """
        self.message =  msg

    def get_message(self):
        """ Returns status message of notifiation. """
        return self.message

    def set_perf_data(self, perfdata):
        """ Adding (service) performance data. 
            
            perfdata - string with performance data
        """
        self.perf_data = '%r' % perfdata 
       
    def get_perf_data(self):
        """ Returns performance data of service (if any) """
        return self.perf_data
    
    def has_perf_data(self):
        """ returns true if performance data is available """

        if self.perf_data:
            return True

    def set_status(self, status = 'UNKNOWN'):
        """ Sets status of service. Valid values are:
        'OK', 'WARNING','CRITICAL', 'UNKNOWN'. 

        status - status of service. default= UNKNOWN, also set
                if wrong values is passed
        """

        if status not in NagiosNotification.STATUS.keys():
            self.status = 'UNKNOWN'
        else:
            self.status = status

    def get_status(self, nsca_coded = True):
        """ returns service status of notification.

            nsca_coded - if set True (default) returned values have 
                        been converted to the standard nagios return 
                        codes (i.e. OK = 0, WARNING = 1 CRITICAL=2, UNKNOWN= -1)
                        if set  False, you get back the strings instead.
        """ 
        if nsca_coded:
            return NagiosNotification.STATUS[self.status]
        else:
            return self.status
        
 

class NagiosNotifier(object):
    """ Nagios notification class. Collects 
        various status messages and notifies Nagios
        server via the nsca client protocol.
    """

    def __init__(self, nagios_server, 
                    send_nsca_cfg = '/etc/send_nsca.cfg', 
                    nsca_bin = '/usr/sbin/send_nsca'):
        """
            nagios_server: FQDN of nagios server e.g. monitor.smscg.ch
            send_nsca_cfg: path to send_nsca client configuration file
            nsca_bin: path to send_nsca binary

            raises NagiosNotifierError if paths do not not exist
        """
        self.log = logging.getLogger(__name__)

        if not os.path.isfile(send_nsca_cfg):
            self.log.error("Could not find send_nsca configuraiton: '%s'" % \
                 send_nsca_cfg)
            raise NagiosNotifierError('nsca config missing',
                "Could not find send_nsca configuraiton: '%s'" % send_nsca_cfg)
        
        if not os.path.isfile(nsca_bin):
            self.log.error("Could not find nsca binary: '%s'" % nsca_bin)
            raise NagiosNotifierError('nsca binary  missing',
                "Could not find nsca binary: '%s'" % nsca_bin)

        self.nagios_server = nagios_server  
        self.nsca_bin = nsca_bin
        self.nsca_cfg = send_nsca_cfg
        self.queue = Queue.LifoQueue(0) # notifications queue

    
    def add_notification(self, notification):
        """ 
        Adds notification to notification queue.
        
        notification - NagiosNotfication object
        """
        self.queue.put(notification)

    def notify(self):
        """ 
        Push notifications to Nagios server. Notice, we only
        sent most recent notification/message for a given
        host-service pair.
        """

        _sent = []  # keep track of already notified

        while not self.queue.empty():
            note = self.queue.get()
            host = note.get_host()
            service = note.get_service()

            if (host,service) in _sent:
                self.log.debug("Skipping old notification for '%s,%s'" % (host, service))
                continue
            _sent.append((host,service))

            nsca_msg = ('%s;%s;%s;%s' % \
                    (host, service, 
                    note.get_status(),
                    note.get_message()))

            if note.has_perf_data():
                nsca_msg += ("|%s" % note.get_perf_data())
    
            nsca_msg += '\n'

            echo = Popen(['echo', '-e', nsca_msg], stdout=PIPE) # beware whitespaces in args not stripped by Popen 
           
            nsca = Popen([self.nsca_bin,  
                        '-H', self.nagios_server, 
                        '-c',  self.nsca_cfg,
                        '-d', ';'], stdin=echo.stdout, stdout=PIPE)

            self.log.debug('Sent notification >%s<.' % nsca_msg)


if __name__ == '__main__':

    # just some quick test 

    LOG_FILENAME = 'nagios_notifier.log'
    
    logging.basicConfig(filename = LOG_FILENAME, level = logging.DEBUG)


    notifier = NagiosNotifier('nagios.smscg.ch')

    note = NagiosNotification('laren.switch.ch','sft_daemon')
    note.set_message("Everything alright")
    note.set_status('OK')
    

    notifier.add_notification(note)
    notifier.notify()


