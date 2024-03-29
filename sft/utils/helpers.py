#!/usr/bin/env python
""" Helper functions and decorators """

import signal
import os
import subprocess
import time
from datetime import datetime 

from  sft.errors.cron import CronError, CronRangeError, CronSyntaxError

from sqlalchemy import and_, desc
import sft.db.sft_meta as meta
import sft.db.sft_schema as schema


def get_sft_test_details(sft_name):
    """ returns the  SFT test description 
        object of SFT with given name (or None)
    """
    return meta.Session.query(schema.SFTTest).\
        filter_by(name=sft_name).first()

def get_sft_tests_details():
    """ returns a list with all SFT test desription
        objects (or empty list)
    """
    return meta.Session.query(schema.SFTTest)

def get_all_sft_names():
    """ returns list with names of all existing SFTs. """
    sft_names = []
    for sft in  get_sft_tests_details():
        sft_names.append(sft.name)
    return sft_names

def get_all_jobs():
    """ returns all SFT jobs in db. """
    return meta.Session.query(schema.SFTJob)

def get_all_sft_jobs(sft_name, cluster_name = None):
    """ returns all jobs of SFT with name  sft_name. If cluster_name is 
        passed only return jobs for given clusters """

    if cluster_name:
        return meta.Session.query(schema.SFTJob).\
            filter(and_(schema.SFTJob.sft_test_name == sft_name,
            schema.SFTJob.cluster_name == cluster_name)).order_by(desc(schema.SFTJob.submissiontime))
    else:
        return meta.Session.query(schema.SFTJob).\
            filter_by(sft_test_name = sft_name).order_by(desc(schema.SFTJob.submissiontime))

def get_cluster_last_sfts(cluster_name):
    """ returns a dictionary  of the latest SFT job results  (each SFT
        once), or None if there weren't any SFT jobs for the specified cluster.
    """

    sfts = {}
    _skipp = [] # keeps track of sft_test_name and test_name that are already in
 
    for job in meta.Session.query(schema.SFTJob).\
    filter_by(cluster_name = cluster_name).\
    order_by(desc(schema.SFTJob.submissiontime)):
        if (job.sft_test_name, job.test_name)  in _skipp:
            pass 
        else:
            if not sfts.has_key(job.sft_test_name):
                sfts[job.sft_test_name] = []

            sfts[job.sft_test_name].append(job)
            _skipp.append((job.sft_test_name, job.test_name))

    return sfts

def get_job(name):
    """ returns SFT job with given name in db. """
    return meta.Session.query(schema.SFTTest).\
            filter_by(name = name).first()

def get_sft_vo_group(vo_group_name):
    """ returns SFT VO group object"""
    return  meta.Session.query(schema.VOGroup).\
                filter_by(name=vo_group_name).first()

def get_sft_cluster_group(cluster_group_name):
    """ returns SFT cluster group object"""
    return meta.Session.query(schema.ClusterGroup).\
                filter_by(name=cluster_group_name).first()

def get_sft_suit(suit_name):
    """ returns SFT suit group object """
    return meta.Session.query(schema.TestSuit).\
                filter_by(name=suit_name).first()

def get_sft_user(DN):
    """ returns SFT user object for given DN. """
    return meta.Session.query(schema.User).filter_by(DN = DN).first()


def get_vo_group_details(group_name):
    """ returns list of VOs objects 
        that are membes of 'group_name'. 
    """
    return meta.Session.query(schema.VOGroup).\
                filter_by(name=group_name).first()

def get_cluster_group_details(group_name):
    """ returns list of cluster objects 
        that are membes of 'group_name'. 
    """
    return meta.Session.query(schema.ClusterGroup).\
                filter_by(name=group_name).first()

def get_test_suit_details(suit_name):
    """ returns list of test suit objects 
        that are membes of 'suit_name'. 
    """
    return meta.Session.query(schema.TestSuit).\
                filter_by(name=suit_name).first()
    
 
def parse_cron_entry(cron_str, _max):
    """
    Parser for *simple* crontab style entries. 
    input: cron_str - crontab like entry as string
             covered cases:
             - integer up to max
             - lists  '1,2,3' 
             - ranges  '1-3'
             - steps   '*/4' or '1-4/2'
             - mix list and range '1,2,3,8-12'
            _max  - maximal allowed value (type integer), must be > 0. Values 
                   greater then max, will be 'reset' to max.
    returns: list of integers (time entries)

    raises: CronRangeError - for entries exceeding valid (time) range
            CronSyntaxError - for syntax errors
            CronError - else
    """
    if type(_max) != int:
        raise CronError("Invalid Max range", 
            "Variable 'max' must be of integer type")     
    if _max <= 0:
        raise CronRangeError("Invalid Max value", 
            "Variable 'max' must be > 0")     
 
    if cron_str.isdigit():
        v = int(cron_str)
        if v > _max:
            return _max
        return v

    if '/' in cron_str: # step
        pre = cron_str.split('/')[0]
        try:
            step = int(cron_str.split('/')[1])
        except:
            raise CronSyntaxError("Syntax Error",
                "'%s' in '%s' must be an integer." % (cron_str.spli('/')[1], cron_str))
        
        if pre == '*':
            return range(0, _max + 1, step)

        if '-' in pre:
            try:
                start = int(pre.split('-')[0])        
                end = int(pre.split('-')[1])
                if end > _max:
                    end = _max
            except:
                raise CronSyntaxError("Syntax Error","'%s' in '%s' must contain an integer." % (pre, cron_str))
            return range(start, end+1, step)
       
    if ('-' in cron_str) and (',' in cron_str):
        res = list()
        for i in cron_str.split(','):
            v = parse_cron_entry(i, _max) 
            if type(v) is int:
                res.append(v)
            else:
                res += v
        if res:
            return res

    if ',' in cron_str:
        res = list()
        for i in cron_str.split(','):
            if i.isdigit():
                v = int(i)
                if v > max:
                    res.append(_max)
                else: 
                    res.append(v)
            else:
                raise CronSyntaxError("Syntax Error","'%s' of '%s' is not a valid entry." % (i, cron_str))
        return res
    
    if '-' in cron_str:
        start = int(cron_str.split('-')[0])        
        end = int(cron_str.split('-')[1])
        if end > _max:
            end = _max
        return range(start, end+1, 1)

    if '*' == cron_str:
        return range(0, _max+1, 1)

    raise CronSyntaxError("Unknown/Invalid syntax", 
            "The cron entry '%s' seems not be a valid crontab style entry!" % (cron_str))



def strip_args(func):
    """ Decorator that strips the input parameters 
        of a function. If arg is a string
        it gets explicitly passed to the function 
        as 'unicode'.
    """
    def new_func(*args, **kwargs):
        args_stripped = list()
        kwargs_stripped = dict()

        for arg in args:
            if type(arg) == str:
                args_stripped.append(unicode(arg.strip()))
            else:
                args_stripped.append(arg)

        if kwargs:
            for k, v in kwargs.items():
                if type(v) == str:
                    kwargs_stripped[k] = unicode(v.strip())
                else:
                    kwargs_stripped[k] = v

        return func(*args_stripped, **kwargs_stripped)
    
    return new_func


class Alarm(Exception):
    pass

def timeout_call_old(cmd, timeout):
    """ Invokes command specified by @cmd. 
        Raises 'Alarm' Exception if cmd does
        not return withint @timeout (in secs)
        
        Can only be used in main thread ;-(
    """
    call = None

    def timeout_handler(signum, frame):
        if call:
            os.kill(call.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
        raise Alarm

    signal.signal(signal.SIGALRM, timeout_handler)

    call = subprocess.Popen(cmd,
            shell = True, close_fds = True,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE)

    signal.alarm(timeout)
    res = call.communicate()
    return_code = call.returncode
    signal.alarm(0) # switch off alarm
    return res, return_code


def timeout_call(cmd, timeout):
    """
    taken from http://amix.dk/blog/post/19408
    call shell-command and either return its output or kill it
    if it doesn't normally exit within timeout seconds and return None"""

    start = datetime.now()
    call = subprocess.Popen(cmd,
            shell = True, close_fds = True,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE)

    while call.poll() is None:
        time.sleep(1)
        now = datetime.now()
        if (now - start).seconds > timeout:
            os.kill(call.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
            raise Alarm

    out, err  = call.communicate()
    return_code = call.returncode
    
    return out, err, return_code



if  __name__ == '__main__':
    
    # some quick helpers testing

    # 1. normal 
    
    print 'testing: 1,2,4  max: 4'
    print parse_cron_entry('1,2,4', 4)
    print 'testing: 3-12  max: 11'
    print parse_cron_entry('3-12', 11)
    print 'testing: */4  max:13'
    print parse_cron_entry('*/4', 13)
    print 'testing: *  max: 7'
    print parse_cron_entry('*', 7)
    print 'testing: 1-4/2 max:13'
    print parse_cron_entry('1-4/2', 13)
    print 'testing: 1,2,3,8-10  max:13'
    print parse_cron_entry('1,2,3,8-10', 13)
    
    # 2. failing 
    try:
        print 'max: 0'
        print parse_cron_entry('1,2,3', 0)
    except Exception, e:
        print 'Error: ', e.msg
    try:
        print '1,a,3 max:5'
        print parse_cron_entry('1,a,3', 5)
    except Exception, e2:
        print 'Error: ', e2.msg
        
    try:
        print 'testing: 1-5,*/12 max: 59'
        print parse_cron_entry('1-5,*/12', 2)
    except Exception, e3:
        print 'Error: ', e3.msg

