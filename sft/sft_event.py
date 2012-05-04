"""
Adaptation from 
http://stackoverflow.com/questions/373335/\
suggestions-for-a-cron-like-scheduler-in-python

"""
__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "11.06.2010"
__version__ = "0.3.0"

import logging
import os.path
import hashlib 
import subprocess
import random


import sft.db.sft_meta as meta
import sft.db.sft_schema as schema
import sft_globals as g # import config, pxhandle, notifier

from errors.sft import *
from nagios_notifier import NagiosNotification
from utils import helpers

class AllMatch(set): 
    """Universal set - match everything"""
    def __contains__(self, item): 
        return True

class SFT_Event(object):
    """ Site Functional Test (SFT) Event. The class holds 
        the time when a specific SFT is scheduled to be run.
    """
    
    def __init__(self, sft_name, minute=None, hour=None,
                       day=None, month=None, dow=None):
        """
        sft_name - name of SFT 
        minute, hour, day, month, dow (day of week) - date parameters

        raises: 
            SFTInvalidExecTime if given date parameters are 
            syntactically wrong
        """
        
        self.log = logging.getLogger(__name__)
        self.sft_name = sft_name
        self.arcsub = '/usr/bin/arcsub'
        self.joblist = os.path.join(g.config.jobsdir, 'jobs.xml')
        self.clusters_down = []

        self.vos = None
        self.clusters = None
        self.tests = None

        allMatch = AllMatch()
        
        if not minute and minute != 0 :
            minute = allMatch
        if not hour and hour != 0:
            hour = allMatch
        if not day and day != 0:
            day = allMatch
        if not month and month != 0:
            month = allMatch
        if not dow and dow != 0:
            dow = allMatch

        try:
            self.mins = self._conv_to_set(minute)
            self.hours = self._conv_to_set(hour)
            self.days = self._conv_to_set(day)
            self.months = self._conv_to_set(month)
            self.dow = self._conv_to_set(dow)
        except:
            raise SFTInvalidExecTime('INVALID_EXEC_TIME',
                "The execution times of the '%s' are invalid." % sft_name)
            
        self._populate_sft_details()
        self._set_arcsub()

        self.log.debug("Initialization finished")


    def _conv_to_set(self, obj):  
        """ converstion to set """
        if isinstance(obj, (int, long)):
            return set([obj]) 
        if not isinstance(obj, set):
            obj = set(obj)
        return obj
    

    def _set_arcsub(self):
        """ Sets path of  arcsub command.

            Raises SFTConfigError if path does not exist or is not a directory
        """
        _arcsub = os.path.join(g.config.arc_clients, 'arcsub') 
        if os.path.isfile(_arcsub):
            self.arcsub = _arcsub
            self.log.debug("'arcsub' command path set to '%s'" % self.arcsub)
        else: 
            raise SFTConfigError("arcsub path error", 
                "'%s' is not a valid file/path" % self.arcsub)
            

    def _populate_sft_details(self):
        """ 
        Populates test structure of SFT, that is
        the involved  VOs, clusters and tests the SFT consists of. 

        raises SFTInalidTestParams for any issue with the SFT test setup.
        """
        sft = helpers.get_sft_test_details(self.sft_name)
        
        if not sft:
            self.log.warn("SFT test '%s' does not exist anymore." % \
                self.sft_name)
            raise SFTInvalidTestParams("SFT missing", 'SFT test does not exist anymore.')
        else:
            vosg = helpers.get_vo_group_details(sft.vo_group)
            if not vosg:
                self.log.warn("SFT test '%s' has no VOs specified." % self.sft_name)
                raise SFTInvalidTestParams("VO missing", 'No VOs associated with SFT.')
            else:
                self.vos = vosg.vos

            clg = helpers.get_cluster_group_details(sft.cluster_group)
            if not clg:
                self.log.warn("SFT test '%s' has no clusters specified." % self.sft_name)
                raise SFTInvalidTestParams('Cluster missing',
                    'No Cluster associated with SFT.')
            else:   
                self.clusters = clg.clusters

            tsts = helpers.get_test_suit_details(sft.test_suit)
            if not tsts:
                self.log.warn("SFT test '%s' has no tests specified." % self.sft_name)
                raise SFTInvalidTestParams('Tests missing',
                    'No test jobs associated with SFT.')
            else:
                self.tests = tsts.tests
        
    
    def _is_exec_time(self, t):
        """Return True if this event should be
            triggered at the specified datetime"""

        self.log.debug("Check whether time  matches")
        return ((t.minute     in self.mins) and
                (t.hour       in self.hours) and
                (t.day        in self.days) and
                (t.month      in self.months) and
                (t.weekday()  in self.dow))


    def _verify_all_sft_users(self):
        """
        Tests whether we can get a myproxy and a vomsproxy 
        for all users associated with this test.
        Notice, we do only call out the myproxy and vomsproxy
        servers, if the credentails of the users have not already
        been fetched previously and/or they are about to expire.    

        returns dictionary with VO (key) and (DN,file paths) tupple  associated
                voms proxy certificates. 
        """
        _vo_dict = {}
        for vo in self.vos:
            for user in vo.users:
                _no_err_flg = True
                DN = user.DN
                passwd = user.get_passwd()
                file_prefix = hashlib.md5(DN).hexdigest()

                myproxy_file = os.path.join(g.config.proxy_dir,
                             file_prefix)
                vomsproxy_file = os.path.join(g.config.proxy_dir,
                            file_prefix + '_' + vo.name) 

                try:
                    g.pxhandle.check_create_myproxy(DN, passwd, myproxy_file)
                    """
                    except ProxyLoadError, epx:
                        # XXX find new identifier...
                        _notification = NagiosNotification(g.config.localhost, self.sft_name )
                        _msg = DN + ': ' + exp.__repr__()
                        _notification.set_message(_msg)
                        _notification.set_status('CRITICAL')
                        g.notifier.add_notification(_notification)
                        _no_err_flg = False

                    except MyProxyError, emy:
                        _notification = NagiosNotification(g.config.localhost, self.sft_name )
                        _msg = DN + ': ' + exp.__repr__()
                        _notification.set_message(_msg)
                        _notification.set_status('CRITICAL')
                        g.notifier.add_notification(_notification)
                        _no_err_flg = False
                    """
                except Exception, exp:
                    _notification = NagiosNotification(g.config.localhost, self.sft_name )
                    _msg = DN + ': ' + exp.__repr__()
                    _notification.set_message(_msg)
                    _notification.set_status('CRITICAL')
                    g.notifier.add_notification(_notification)
                    _no_err_flg = False
                    continue
                
                try:                
                    g.pxhandle.check_create_vomsproxy(DN, file_prefix, vo.name)
                except Exception, exp2:
                    _notification = NagiosNotification(g.config.localhost, self.sft_name )
                    _msg = DN + ': ' + exp2.__repr__()
                    _notification.set_message(_msg)
                    _notification.set_status('CRITICAL')
                    g.notifier.add_notification(_notification)
                    _no_err_flg = False
                
                if _no_err_flg:
                    _notification = NagiosNotification(g.config.localhost, self.sft_name )
                    _msg = 'myproxy and voms_proxy for DN:' + DN
                    _notification.set_message(_msg)
                    _notification.set_status('OK')
                    g.notifier.add_notification(_notification)
                    if not _vo_dict.has_key(vo.name):
                        _vo_dict[vo.name] = []
                    _vo_dict[vo.name].append((DN, vomsproxy_file))

        return _vo_dict
    
    def get_name(self):
        """ returns event name """
        return self.sft_name

    def set_clusters_down(self, clusters):
        """ Set's the list of clusters that are currently
            on scheduled downtime. 
        """
        self.clusters_down = clusters

    def check_exec(self, t):
        """ checks whether it's time to execute sft event. """
        
        if self._is_exec_time(t):
            self.log.debug("Time matched, SFT will be carried out.") 
            #session = meta.Session()
            session = meta.Session
            _vo_dict = self._verify_all_sft_users()
            self.log.debug("got _vo_dict '%r'" % _vo_dict)

            _commit_flg = False
            for vo_name in _vo_dict.keys():
                DN, voms_proxy_file  = random.choice(_vo_dict[vo_name]) # random select one user
                os.putenv('X509_USER_PROXY', voms_proxy_file)
 
                for cluster in self.clusters:
                    if cluster.hostname in self.clusters_down:
                        continue
                    for test in self.tests:
                        sft_job = schema.SFTJob(self.sft_name)
                        sft_job.cluster_name = cluster.hostname
                        sft_job.DN = DN
                        sft_job.vo_name = vo_name
                        sft_job.test_name = test.name

                        xrsl_str = test.xrsl.replace('\n',' ')
                        
                        cmd = "%s -j %s -c %s -e '%s'" % \
                            (self.arcsub, self.joblist, cluster.hostname, xrsl_str)
                        arcsub = subprocess.Popen( cmd,
                            shell = True,
                            stdout = subprocess.PIPE,
                            stderr = subprocess.PIPE)
                    
                        """    
                        arcsub = subprocess.Popen(
                            [ self.arcsub,
                            '-j', self.joblist,
                            '-c', cluster.hostname,
                            '-e', xrsl_str],
                            shell = True,
                            stdout = subprocess.PIPE,
                            stderr = subprocess.PIPE)
                        cmd = "%s -c %s -e '%s'" % \
                            (self.arcsub, cluster.hostname, test.xrsl.replace('\n',' '))
                        self.log.debug('cmd:>%s<' % cmd)
                        ret = subprocess.Popen(cmd, shell=True, 
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        ret.wait()
                        ret.poll()
                        """

                        output, stderr = arcsub.communicate()
                        if arcsub.returncode == 0:
                            # additional check -> if cluster does not exists, we still get returncode =0
                            if 'Job submission failed due to' in output:    
                                self.log.error("(%s) - job submission failed (retcode is 0)" % test.name)
                                sft_job.status = 'failed'
                                sft_job.error_msg = output
                                session.add(sft_job)
                                session.flush()
                                _commit_flg = True
                            
                                _notification = NagiosNotification(cluster.hostname, self.sft_name )
                                _msg = output # XXX maybe add VO + DN + test to get more details
                                _notification.set_message(_msg)
                                _notification.set_status('CRITICAL')
                                g.notifier.add_notification(_notification)
                                break
                            else: 
                                jobid = output.split('jobid:')[1].strip()
                                sft_job.jobid = jobid
                                self.log.debug("(%s)- job sumbitted: ID %s" % (test.name, jobid))
                                sft_job.status = 'submitted'
                                session.add(sft_job)
                                session.flush()
                                _commit_flg = True
                            
                                _notification = NagiosNotification(cluster.hostname, self.sft_name )
                                _msg = '(%s) - successfully submitted' % (test.name)
                                _notification.set_message(_msg)
                                _notification.set_status('OK')
                                g.notifier.add_notification(_notification)
                                continue
                        else:
                            self.log.error("(%s) Job submission failed, with: %s" % (test.name, stderr))
                            sft_job.error_type = "arcsub"
                            sft_job.error_msg =  stderr
                            sft_job.status = 'failed'
                            session.add(sft_job)
                            session.flush()
                            _commit_flg = True
                            
                            _notification = NagiosNotification(cluster.hostname, self.sft_name )
                            _msg = '(%s) - %s' % (test.name, stderr)
                            _notification.set_message(_msg)
                            _notification.set_status('CRITICAL')
                            g.notifier.add_notification(_notification)
                            
            if _commit_flg:
                session.commit()
