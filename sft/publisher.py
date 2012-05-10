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

import db.sft_meta as meta
import db.sft_schema as schema

import sft_globals as g 
from errors import publisher
from nagios_notifier import NagiosNotification

from indexer import HTMLIndexer, HTMLIndexerError
import sft.db.sft_schema as schema

__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "20.04.2010"
__version__ = "0.2.0"

class Publisher(object):
    """ Queries for submitted jobs and fetches them
        if jobs have completed.
    """

    GRID_FIN_STATES = [ 'FAILED', 'FINISHED', 
                'DELETED', 'KILLED'] 

    FIN_STATES = ['failed', 'fetched', 'fetched_failed', 
                'timeout'] + GRID_FIN_STATES

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.session = meta.Session
        self.jobsdir = g.config.jobsdir
        self.joblist = os.path.join(self.jobsdir, 'jobs.xml')
        self.log.info("Jobs download directory set to '%s'" % self.jobsdir)
        try:
            self.html_indexer = HTMLIndexer(g.config.url_root) 
        except HTMLIndexerError, e: 
            self.log.error("%s:  %s" % ( e.expression, e.message))
        
        _arcstat = os.path.join(g.config.arc_clients, 'arcstat') 
        _arcget = os.path.join(g.config.arc_clients, 'arcget') 
        if os.path.isfile(_arcstat):
            self.arcstat = _arcstat
            self.log.debug("'arcstat' command path set to '%s'" % self.arcstat)
        else:
            raise publisher.PublisherError("arcstat path error",
                "'%s' is not a valid file/path" % self.arcstat)

        if os.path.isfile(_arcget):
            self.arcget = _arcget
            self.log.debug("'arcget' command path set to '%s'" % self.arcget)
        else:
            raise publisher.PublisherError("arcget path error",
                "'%s' is not a valid file/path" % self.arcget)


        self.pos_dn_vos = []
        self.neg_dn_vos = []

        self.log.debug("Initialization finished")


    def reset_proxy_cache(self):
        """ resets (user,VO) caches. """
        self.pos_dn_vos = []
        self.neg_dn_vos = []


    def __set_x509_user_proxy(self, DN, vo_name):
        """
        Try to set X509_USER_PROXY environment variable
        to a vomsproxy for user DN and VO vo.
        returns True, if environment variable could be set
                False, else
        """
        if (DN, vo_name) in self.neg_dn_vos:
            return False
        
        user = self.session.query(schema.User).filter_by(DN=DN).first()
        if not user:
            return False
        passwd = user.get_passwd()
        file_prefix = hashlib.md5(DN).hexdigest() 
        myproxy_file = os.path.join(g.config.proxy_dir, file_prefix)
        voms_proxy_file = os.path.join(g.config.proxy_dir, 
                file_prefix +'_'+vo_name) 

        if (DN, vo_name) not in self.pos_dn_vos:
            try:
                g.pxhandle.check_create_myproxy(DN, passwd, myproxy_file)
                g.pxhandle.check_create_vomsproxy(DN, file_prefix, vo_name)
            except Exception, exp2:
                _notification = NagiosNotification(g.config.localhost, 'publisher' )
                _msg = DN + ': ' + exp2.__repr__()
                _notification.set_message(_msg)
                _notification.set_status('CRITICAL')
                g.notifier.add_notification(_notification)
                self.neg_dn_vos.append((DN, vo_name))
                return False   
            
            self.pos_dn_vos.append((DN, vo_name))

        os.environ['X509_USER_PROXY'] = voms_proxy_file
        return True


    def check_submitted_jobs(self):
        """ checking whether submitted jobs can be fetched."""
       
        for entry in self.session.query(schema.SFTJob).\
            filter(AND(schema.SFTJob.status != 'failed',
                schema.SFTJob.status != 'fetched',
                schema.SFTJob.status != 'success',
                schema.SFTJob.status != 'fetched_failed',
                schema.SFTJob.status != 'fetch_failed',
                schema.SFTJob.status != 'test_failed',
                schema.SFTJob.status != 'timeout',
                schema.SFTJob.status != 'FAILED',
                schema.SFTJob.status != 'FINISHED',
                schema.SFTJob.status != 'KILLED',
                schema.SFTJob.status != 'DELETED')).all():

            self.log.debug("Checking job in state: %s" % entry.status)
            
            DN = entry.DN
            vo_name = entry.vo_name 
            if not self.__set_x509_user_proxy(DN, vo_name):
                continue 

            self.log.debug("Querying status of job: %s" % entry.jobid.strip())

            arcstat = subprocess.Popen(
                [ self.arcstat,
                '-j', self.joblist,
                entry.jobid.strip()],
                stdout = subprocess.PIPE,
                stderr = subprocess.PIPE)

            stdoutdata, stderrdata = arcstat.communicate()
            if arcstat.returncode == 0:
                output = stdoutdata.split('\n')  # Job, Job Name, Status, [Exit code]
                if len(output) > 2:
                    # some dirty hacking, as Status sometimes not present, no clue why.
                    if not output[2]:
                        continue
                    status = output[2].split('(')[1].strip(') ') # state is presented in ()
                    self.log.debug("Refreshed job status:>%s<" % status)
                else:
                    self.log.error("Parsing job status: '%s'" % output)
                    continue
                entry.status = status
                entry.db_lastmodified = datetime.utcnow()
                self.session.flush()
            else:
                _error_msg = stdoutdata + 'Error: ' + stderrdata
                self.log.debug("Quering job status failed with %s" % _error_msg)
               
                # hack to intercept jobs that got 'lost'
                if 'No jobs given' in stderrdata:
                    entry.status = 'fetch_failed'
                    entry.error_type = 'sft'
                    entry.error_msg = "Job '%s' not found anymore" % entry.jobid.strip()
 
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
            vo_name = entry.vo_name 
            if not self.__set_x509_user_proxy(DN, vo_name):
                continue 

            nstatus = self.fetch_job(jobid)
            self.log.debug("Fetching job status: %s" % nstatus)
            if nstatus == 'fetched':
                outdir = os.path.join(self.jobsdir, jobid.split('/jobs/')[1])
                if entry.status == 'FAILED':
                    entry.status = 'fetched_failed'
                    entry.error_type = 'lrms'
                    entry.error_msg = "Feching job '%s' failed" % jobid
                else:
                    # check whether test logically failed.
                    entry.status, entry.error_type, entry.error_msg  = self.check_test_succeeded(outdir) 
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
                entry.status = 'fetch_failed'
                entry.error_type = 'lrms'
                entry.error_msg = 'Job could not be retrieved anymore '


            _notification = NagiosNotification(entry.cluster_name, entry.sft_test_name)
            if entry.status == 'success':
                _notification.set_status('OK')
                _msg = '(%s) successfully executed' % entry.test_name
            else:
                _notification.set_status('CRITICAL')
                _msg = '(%s) execution faile' % entry.test_name
            _notification.set_message(_msg)
            g.notifier.add_notification(_notification)
        
        self.session.commit()


    def fetch_job(self, jobid):
        """ fetching the specified job. 
            Returns: fetched - if job could be fetched
                     failed  - is something went wrong. 
            
            if failed: with get_last_error(), an error message 
            can be fetched. 
        """    
        cmd = '%s -j %s -D %s %s' % (self.arcget, self.joblist, self.jobsdir, jobid.strip() )
        
        # this did not work... why?
        """
        arcget  = subprocess.Popen(
            [self.arcget,
            '-j', self.joblist,
            '-D', self.jobsdir,
            jobid.strip()],  shell ...
        """
        arcget = subprocess.Popen(cmd,
            shell = True, 
            stdout = subprocess.PIPE, 
            stderr = subprocess.PIPE)
    
        # as ret.wait() sets return codes != 0 even for success, we therefore
        # need to parse the output ;-(
        
        output, stderr = arcget.communicate()
        self.log.debug("arcget output:>%s<, stderr:>%s<" % (output.strip('\n'), stderr))
    
        self.log.debug("return-code: %d" % arcget.returncode)
 
        if output and ('successfully' in output):
            self.log.info("Stored job results at %s" % output.strip('\n'))
            return 'fetched'
        else:
            _error_msg = stderr
            self.log.error("Fetching job '%s' failed with %s" % 
                (jobid, _error_msg))
            # XXX need to intercept different kind of errors -> deal with them individually
            return 'failed'
    
    def check_test_succeeded(self, outdir):
        """
        Checking whether the site functional test of given output directory
        was 'logically' successful.
        Underlying assumptions are, that the test was written following one of the
        following conventions (optoins):
        
        option a.) in xrls of job: (stderr = 'error.log'
        option b.) in xrls of job: (gmlog = '.arc')
        option c.) in xrls of job: (gmlog = 'log')

        and that the output directory exists

        returns 'success', None, None if job got executed correctly
                'test_failed', error_type, error_msg,  if job got executed by failed

        """
        if not os.path.isdir(outdir):
            return 'test_failed', 'output', 'No job output directory'
        
        if '.arc' in os.listdir(outdir):
            _gmlog = os.path.join(outdir, '.arc')
        elif 'log' in os.listdir(outdir):
            _gmlog = os.path.join(outdir, 'log')
        else:
            _gmlog = None
       
 
        if 'error.log' in os.listdir(outdir):
            elog = os.path.join(outdir, 'error.log')
            if os.path.getsize(elog) > 0:
                return 'test_failed', 'logical', 'Test failed, see error.log for details'
            if not _gmlog:
                return 'success', None, None
                 
       
        self.log.debug('Checking gmlog file: >%s< in output dir: %s' % (_gmlog, outdir)) 

        if not _gmlog or  not os.path.isdir(_gmlog):
            return 'test_failed', 'output', "gmlog missing, or it's not set to '.arc' or 'log'."
            
        if 'failed' in os.listdir(_gmlog):
            return 'test_failed', 'logical', "Test failed, see gmlog 'failed' file"
            
        return 'success', None, None
           

    def main(self):
        """ main method """
        self.reset_proxy_cache()
        self.check_submitted_jobs()
        self.fetch_final_jobs()

 
