"""
Adaptation from 
http://stackoverflow.com/questions/373335/suggestions-for-a-cron-like-scheduler-in-python

"""
__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "11.06.2010"
__version__ = "0.2.0"

import logging
import db.sft_meta as meta
import db.sft_schema as schema
from sqlalchemy import orm
import os, os.path, hashlib
from utils.myproxy_vomsproxy import  ProxyUtil
import subprocess


class AllMatch(set): 
    """Universal set - match everything"""
    def __contains__(self, item): return True

allMatch = AllMatch()

def conv_to_set(obj):  
    """ converstion to set """
    if isinstance(obj, (int, long)):
        return set([obj]) 
    if not isinstance(obj, set):
        obj = set(obj)
    return obj

class SFT_Event(object):
    """ Site Functional Test (SFT) Event. The class holds the time when
        a specific SFT is scheduled to be run.
     """

    def __init__(self, sft_name, minute=None, hour=None,
                       day=None, month=None, dow=None):

        if not minute:
            minute = allMatch
        if not hour:
            hour = allMatch
        if not day:
            day = allMatch
        if not month:
            month = allMatch
        if not dow:
            dow = allMatch

        self.log = logging.getLogger(__name__)
        self.mins = conv_to_set(minute)
        self.hours = conv_to_set(hour)
        self.days = conv_to_set(day)
        self.months = conv_to_set(month)
        self.dow = conv_to_set(dow)
        self.sft_name = sft_name
        self.ngsub = '/opt/nordugrid/bin/ngsub'  # default
        Session = orm.scoped_session(meta.Session)
        self.session = Session()
        self.proxy_util = ProxyUtil()
        self.last_error_msg = None
        self.log.debug("Initialization finished")

    def set_ngsub_path(self, path):
        """ overwrite default path for ngsub command """
        if path:
            self.ngsub = os.path.join(path, 'ngsub') 
            self.log.info("'ngsub' command path set to '%s'" % self.ngsub)

    def matchtime(self, t):
        """Return True if this event should trigger at the specified datetime"""
        self.log.debug("Check whether time  matches")
        return ((t.minute     in self.mins) and
                (t.hour       in self.hours) and
                (t.day        in self.days) and
                (t.month      in self.months) and
                (t.weekday()  in self.dow))


    def get_sft_details(self):
        """ return VOs, clusters and tests the SFT consists of. """
        vos = None
        clusters = None
        tests = None

        sft = self.session.query(schema.SFTTest).\
            filter_by(name=self.sft_name).first()
        if not  sft:
            self.log.warn("SFT test '%s' does not exist anymore." % \
                self.sft_name)
        else:
            vosg = self.session.query(schema.VOGroup).\
                filter_by(name=sft.vo_group).first()
            if not vosg:
                self.log.warn("SFT test '%s' has no VOs specified." % self.sft_name)
            else:
                vos = vosg.vos

            clg = self.session.query(schema.ClusterGroup).filter_by(name=sft.cluster_group).first()
            if not clg:
                self.log.warn("SFT test '%s' has no clusters specified." % self.sft_name)
            else:
                clusters = clg.clusters

            tsts = self.session.query(schema.TestSuit).filter_by(name=sft.test_suit).first()
            if not tsts:
                self.log.warn("SFT test '%s' has no tests specified." % self.sft_name)
            else:
                tests = tsts.tests
        return vos, clusters, tests

    def get_name(self):
        """ returns event name """
        return self.sft_name

    def get_last_error(self):
        """ returns very last error that occurred """
        return self.last_error_msg

    def check_exec(self, t):
        """ checks whether it's time to execute sft event. """
        if self.matchtime(t):
            vos, clusters, tests = self.get_sft_details()
            # fetch representative user(s) for each VO.
            for vo in vos:
                myproxy_error = False
                vomsproxy_error = False
                for user in vo.users:
                    DN = user.DN
                    passwd = user.get_passwd()
                   
                    file_prefix = hashlib.md5(DN).hexdigest()
                    myproxy_file = os.path.join(self.proxy_util.get_proxy_dir(),
                                     file_prefix)
                    vomsproxy_file = os.path.join(self.proxy_util.get_proxy_dir(), 
                                    file_prefix + '_' + vo.name) # proxy file
                    
                    if not self.proxy_util.check_create_myproxy(DN, passwd, myproxy_file):
                        myproxy_error = True
                        continue    
                    if not self.proxy_util.check_create_vomsproxy(DN, file_prefix, vo.name):
                        vomsproxy_error = True
                        continue

                    # submit jobs to cluster            
                    os.putenv('X509_USER_PROXY', vomsproxy_file)
                    for cluster in clusters:
                        for test in tests:
                            sft_job = schema.SFTJob(self.sft_name)
                            sft_job.cluster_name = cluster.hostname
                            sft_job.DN = DN
                            sft_job.vo_name = vo.name
                            sft_job.test_name = test.name
                            cmd = "%s -c %s -e '%s'" % \
                                (self.ngsub, cluster.hostname, test.xrsl.replace('\n',' '))
                            ret = subprocess.Popen(cmd, shell=True, 
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                            ret.wait()
                            ret.poll()
                            if ret.returncode == 0:
                                # additional check -> if cluster does not exists, we still get returncode =0
                                output = ret.communicate()
                                if 'Job submission failed due to' in output[0]:    
                                    self.log.error("Job submission failed (retcode is 0)")
                                    sft_job.status = 'failed'
                                    sft_job.error_msg = output[0]
                                    self.session.add(sft_job)
                                    self.session.flush()
                                    break
                                else: 
                                    jobid = output[0].split('jobid:')[1].strip()
                                    sft_job.jobid = jobid
                                    self.log.debug("Job sumbitted: ID %s" % (jobid))
                                    sft_job.status = 'submitted'
                            else:
                                self.last_error_msg = ret.communicate()[0]
                                self.log.error("Job submission failed.")
                                sft_job.error_type = "ngsub"
                                sft_job.error_msg = self.get_last_error()
                                sft_job.status = 'failed'
                            self.session.add(sft_job)
                            self.session.flush()
                    break # if one user succeeded we stop
                # end user-loop 
                if myproxy_error:
                    sft_job = schema.SFTJob(self.sft_name, 
                        error_type = "myproxy", error_msg=self.proxy_util.get_last_error())
                    sft_job.status = 'failed'
                    sft_job.vo_name = vo.name
                if vomsproxy_error:    
                    sft_job = schema.SFTJob(self.sft_name, 
                        error_type = "vomsproxy", error_msg=self.proxy_util.get_last_error())
                    sft_job.status = 'failed'
                    sft_job.vo_name = vo.name
                if myproxy_error or vomsproxy_error:
                    self.session.add(sft_job)
                    self.session.flush()
                
            # end vo loop
            self.session.commit()
