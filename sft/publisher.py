#!/usr/bin/env python
"""
Deals with Querying and fetching SFT jobs 
that got submitted.
"""
import logging
import os, os.path,  hashlib
import subprocess
from sqlalchemy import and_ as AND
from sqlalchemy import or_ as OR
from datetime import datetime


from utils.myproxy_vomsproxy import  ProxyUtil
from indexer import HTMLIndexer, HTMLIndexerError
import sft.db.sft_meta as meta
import sft.db.sft_schema as schema

__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "20.04.2010"
__version__ = "0.1.0"

class Publisher(object):
    """ Queries for submitted jobs and fetches them
        if jobs have completed.
    """

    GRID_FIN_STATES = [ 'FAILED', 'FINISHED', 
                'DELETED', 'KILLED'] 

    FIN_STATES = ['failed', 'fetched', 'fetched_failed', 
                'timeout'] + GRID_FIN_STATES

    def __init__(self, jobsdir, url_root):
        self.log = logging.getLogger(__name__)
        self.jobsdir = jobsdir
        self.log.info("Jobs download directory set to '%s'" % self.jobsdir)
        try:
            self.html_indexer = HTMLIndexer(url_root) 
        except HTMLIndexerError, e: 
            self.log.error("%s:  %s" % ( e.expression, e.message))
            
        self.ngstat = '/opt/nordugrid/bin/ngstat' # default        
        self.ngget = '/opt/nordugrid/bin/ngget' # default        
        
        self.session = meta.Session()
        self.proxy = ProxyUtil()
        self.pos_dn_vos = list() # success to get proxy for (user, VO) list
        self.neg_dn_vos = list() # failed to get proxy for (user,  VO) list
        self.last_error_msg = None
        self.log.debug("Initialization finished")

    def set_ngstat_ngget_path(self, path):
        """ overwrite default path for ngsub command """
        if path:
            self.ngstat = os.path.join(path, 'ngstat') 
            self.log.info("'ngstat' command path set to '%s'" % self.ngstat)
            self.ngget = os.path.join(path, 'ngget') 
            self.log.info("'ngget' command path set to '%s'" % self.ngget)

    def reset_proxy_cache(self):
        """ resets (user,VO) caches. """
        self.pos_dn_vos = list()
        self.neg_dn_vos = list()

    def get_last_error_msg(self):
        """ returns very last error that occurred """
        return self.last_error_msg

    def check_submitted_jobs(self):
        """ checking whether submitted jobs can be fetched."""
       
        for entry in self.session.query(schema.SFTJob).\
            filter(AND(schema.SFTJob.status != 'failed',
                schema.SFTJob.status != 'fetched',
                schema.SFTJob.status != 'fetched_failed',
                schema.SFTJob.status != 'timeout',
                schema.SFTJob.status != 'FAILED',
                schema.SFTJob.status != 'FINISHED',
                schema.SFTJob.status != 'KILLED',
                schema.SFTJob.status != 'DELETED')).all():
            DN = entry.DN
            jobid = entry.jobid.strip()
            user = self.session.query(schema.User).filter_by(DN=DN).first()
            if not user:
                continue
            passwd = user.get_passwd()
            vo_name = entry.vo_name 
            file_prefix = hashlib.md5(DN).hexdigest() 
            myproxy_file = os.path.join(self.proxy.get_proxy_dir(), file_prefix)
            vomsproxy_file = os.path.join(self.proxy.get_proxy_dir(), 
                    file_prefix +'_'+vo_name) 

            if (DN, vo_name) in self.neg_dn_vos:
                continue
            elif (DN, vo_name) not in self.pos_dn_vos:
                if not self.proxy.check_create_myproxy(DN, passwd, myproxy_file):
                    self.neg_dn_vos.append((DN, vo_name))
                    continue    
                if not self.proxy.check_create_vomsproxy(DN, file_prefix, vo_name):
                    self.neg_dn_vos.append((DN, vo_name))
                    continue
                self.pos_dn_vos.append((DN, vo_name))

            os.putenv('X509_USER_PROXY', vomsproxy_file)
            cmd = "%s %s" % (self.ngstat, jobid)
            ret = subprocess.Popen(cmd, shell=True, 
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            ret.wait()
            ret.poll()
            if ret.returncode == 0:
                output = ret.communicate()[0].split('\n')  # Job, Job Name, Status, [Exit code]
                if len(output) > 2:
                    status = output[2].split('Status:')[1].strip()
                else:
                    self.log.error("Parsing job status: '%s'" % output)
                    continue
                self.log.debug("Got job status: '%s'" % (status))
                entry.status = status
                entry.db_lastmodified = datetime.utcnow()
                self.session.flush()
            else:
                self.last_error_msg = ret.communicate()[0]
                self.log.error("Quering job status failed with %s" % self.last_error_msg)
        self.session.commit()

    def fetch_final_jobs(self):
        """ fetching all jobs in final state, that were not yet fetched. """
        
        for entry in self.session.query(schema.SFTJob).\
            filter(OR(schema.SFTJob.status == 'FAILED',
                schema.SFTJob.status == 'FINISHED')).all():
            # since filter is case insensitive, let's skip 'failed' status
            jobid = entry.jobid
            if not jobid:
                continue
            jobid = jobid.strip()
            DN = entry.DN
            user = self.session.query(schema.User).filter_by(DN=DN).first()
            if not user:
                continue
            passwd = user.get_passwd()
            vo_name = entry.vo_name 
            file_prefix = hashlib.md5(DN).hexdigest() 
            myproxy_file = os.path.join(self.proxy.get_proxy_dir(), file_prefix)
            vomsproxy_file = os.path.join(self.proxy.get_proxy_dir(), file_prefix +'_'+vo_name) 

            if (DN, vo_name) in self.neg_dn_vos:
                continue
            elif (DN, vo_name) not in self.pos_dn_vos:
                if not self.proxy.check_create_myproxy(DN, passwd, myproxy_file):
                    self.neg_dn_vos.append((DN, vo_name))
                    continue    
                if not self.proxy.check_create_vomsproxy(DN, file_prefix, vo_name):
                    self.neg_dn_vos.append((DN, vo_name))
                    continue
                self.pos_dn_vos.append((DN, vo_name))

            os.putenv('X509_USER_PROXY', vomsproxy_file)
            nstatus = self.fetch_job(jobid)
            if nstatus == 'fetched':
                outdir = os.path.join(self.jobsdir, jobid.split('/jobs/')[1])
                if entry.status == 'FAILED':
                    entry.status = 'fetched_failed'
                    entry.error_type = 'lrms'
                    entry.error_msg = "Job failed. For more details check output."
                else:
                    entry.status = nstatus
                try:
                    self.html_indexer.set_path(outdir)
                    self.html_indexer.generate()
                    entry.outputdir = self.html_indexer.get_logical_path()
                except HTMLIndexerError, e: 
                    self.log.error("%s:  %s" % ( e.expression, e.message))
                    entry.outputdir = outdir + '(indexer error)' 
                entry.db_lastmodified = datetime.utcnow()
                self.session.flush()
                
            else: 
                self.log.error("Fetching %s failed with %s" % (jobid, self.get_last_error_msg()))
        
        self.session.commit()

    def fetch_job(self, jobid):
        """ fetching the specified job. 
            Returns: fetched - if job could be fetched
                     failed  - is something went wrong. 
            
            if failed: with get_last_error(), an error message 
            can be fetched. 
        """    
        
        cmd = "%s -dir %s  %s" % (self.ngget, self.jobsdir, jobid.strip())
        ret = subprocess.Popen(cmd, shell=True, 
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        ret.wait()
        ret.poll()
        if ret.returncode == 0:
            output = ret.communicate()[0]
            self.log.info("Stored job results at %s" % output.strip('\n'))
            return 'fetched'
        else:
            self.last_error_msg = ret.communicate()[0]
            self.log.error("Fetching job '%s' failed with %s" % 
                (jobid,self.last_error_msg))
            return 'failed'
            

    def main(self):
        """ main method """
        self.check_submitted_jobs()
        self.fetch_final_jobs()
        self.reset_proxy_cache()

 
