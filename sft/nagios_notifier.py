"""
Nagios notification module. Uses the curl  protocol to 
push SFT notifications to a nagios server. On the
nagios server site we recommend to install 
the NSCAweb (http://wiki.smetj.net/wiki/Nscaweb) module, as
we will submit multi-line notifications, which
will are fed to  the message to the nagios.cmd pipe.

The messages are received there as passive tests.

"""
__author__ = "Placi Flury grid@switch.ch"
__date__ = "16.02.2012"
__version__ = "0.2.0"

import time
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
        server via the curl protocol.
    """

    def __init__(self, config):
        """
            config - global config object 
        """
        self.log = logging.getLogger(__name__)

        self.curl_bin = config.curl_bin

        self.nscaweb_endpoint = config.nscaweb_host + ':' + \
                str(config.nscaweb_port) +'/queue/' + \
                 config.nscaweb_queue
        self.nscaweb_port = config.nscaweb_port
        self.nscaweb_user = config.nscaweb_user
        self.nscaweb_pwd = config.nscaweb_pwd

        self.queue = Queue.LifoQueue(0) # notifications queue

    
    def add_notification(self, notification):
        """ 
        Adds notification to notification queue.
        
        notification - NagiosNotfication object
        """
        self.queue.put(notification)

    def notify(self, trace=True):
        """ 
        Push notifications to nscaweb host (usually running nagios server). 

        params:
        trace - if set true (default), the notifications for the same 
                host/service pairs will be sent as chronological
                traces (like stack traces).
                if set false, only the most recent  notification for
                a host/service pair will be sent (older notifications
                are masked out). 
        """
        # curl_msg: [TIMESTAMP] COMMAND_NAME;argument1;argument2;...;argumentN
        if not trace:
            pass
        else: 
            hs_msg_stack = {}
            hs_perf_data = {}
            hs_fin_status = {}

            while not self.queue.empty():
                _note = self.queue.get()
                _status = _note.get_status()
                
                
                key = (_note.get_host(), _note.get_service())   
            
                if not hs_msg_stack.has_key(key):
                    hs_msg_stack[key] = []
                    hs_fin_status[key] = _status
                    if _note.has_perf_data():
                        hs_perf_data[key] = _note.get_perf_data()

                _msg = _note.get_status(nsca_coded=False) + ': ' +  _note.get_message()
                hs_msg_stack[key].append(_msg)
                
                # fin status changes from OK to WARN for any non-OK sub-test that 
                # had any problem
                if hs_fin_status[key] == 0 and _status != 0:
                    hs_fin_status[key] = 1

            for k in hs_msg_stack.keys():

                timestamp = int(time.time())

                curl_msg = ('[%d] PROCESS_SERVICE_CHECK_RESULT;%s;%s;%s;%s' % \
                        (timestamp, k[0], k[1], 
                        hs_fin_status[k],
                        hs_msg_stack[k]))

                if hs_perf_data.has_key(k):
                    curl_msg += ("|%s" % hs_perf_data[k])

                self.log.debug("username=%s'" % self.nscaweb_user) 
 
                Popen([self.curl_bin,  
                            '-d',  'username=%s' %  self.nscaweb_user, 
                            '-d',  'password=%s' % self.nscaweb_pwd, 
                            '--data-urlencode', 
                             "input=%s" % curl_msg ,
                            self.nscaweb_endpoint], stdout=PIPE)

                self.log.debug('Sent notification >%s<.' % curl_msg)
                
            

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


