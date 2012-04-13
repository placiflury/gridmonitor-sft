"""
Myproxy and vomsproxy utility class
"""
__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "11.03.2010"
__version__ = "0.2.0"

import logging
import os
import os.path
import stat
from M2Crypto import X509
import time
import calendar
import subprocess

from errors.proxy import *
from sft.utils import myproxyclient

class ProxyUtil(object):
        
    def __init__(self, config):
        self.log = logging.getLogger(__name__)
        self.px_dir = config.proxy_dir
        self.px_type = config.proxy_type

        self.min_vomsproxy_hours = config.min_voms_proxy_hours
        self.min_myproxy_hours = config.min_myproxy_hours 
        
        self.myproxy = myproxyclient.MyProxyClient(config.myproxy_server,
                config.myproxy_port)

        self.myproxy.set_proxy_type(self.px_type, config.proxy_policy)
        
    
    def get_proxy_dir(self):
        """ return proxy directory """
        return self.px_dir        

    def check_create_vomsproxy(self, DN, file_prefix, vo_name):
        """
        checks whether proxy for VO exists already and whether it's still valid. if not
        it will try to create a new proxy. 
        raises VomsProxyError, ProxyLoadError
        """
        ckfile = os.path.join(self.px_dir, file_prefix) # key + cert file
        pxfile = os.path.join(self.px_dir, file_prefix + '_' + vo_name) # proxy file

        if os.path.exists(pxfile) and os.path.isfile(pxfile):
            try:
                x509 = X509.load_cert(pxfile, X509.FORMAT_PEM)
                enddate = x509.get_not_after().__str__()
            except Exception, e: 
                raise ProxyLoadError('Loading Error', "Loading voms_proxy '%s' got '%s'" % (pxfile, e))

            enddate_tuple = time.strptime(enddate,'%b %d %H:%M:%S %Y GMT')
            enddate_epoch = calendar.timegm(enddate_tuple)
            remaining_hours = (enddate_epoch - int(time.time()))/3600
            self.log.debug("VOMS proxy remainig hours: %d " % remaining_hours)
            self.log.debug("VOMS proxy minimal  hours: %d " % self.min_vomsproxy_hours)
       
            if remaining_hours > self.min_vomsproxy_hours:
                return 
       
        try:
            if self.px_type == 'old':
                cmd ="voms-proxy-init -voms %s -key %s -cert %s -old -hours 10 -out %s" % \
                 (vo_name, ckfile, ckfile, pxfile)
            else: # use 'rfc' type
                cmd ="voms-proxy-init -voms %s -key %s -cert %s -rfc -hours 10 -out %s" % \
                 (vo_name, ckfile, ckfile, pxfile)

            ret = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            ret.poll()
            if ret.returncode == 0:
                self.log.debug("Got voms proxy for %s (VO: %s)" % (DN, vo_name))
                os.chmod(pxfile, stat.S_IRUSR | stat.S_IWUSR) # else ngsub complains
                return 
            else:
                # Warnings have return codes != 0, here's a hack to intercept them
                ret_msg = ret.communicate()
                if ('Warning:' in ret_msg[0]) or ('Your proxy is valid until' in ret_msg[0]):
                    self.log.debug("Got voms proxy for %s (VO: %s) with warning." % (DN, vo_name))
                    os.chmod(pxfile, stat.S_IRUSR | stat.S_IWUSR) # else ngsub complains
                    return 
         
                error = ret_msg
                self.log.error("Requesting voms proxy for '%s' (VO: %s), got: %s" %
                    (DN, vo_name, error))
                raise VomsProxyError("VomsProxy Error.",
                    "Requesting voms proxy for '%s' (VO: %s), got: %s" %
                    (DN, vo_name, error))

        except Exception, e:
            self.log.error("Could not get voms proxy cert for '%s', got '%s'" % (DN, e))
            raise VomsProxyError("VomsProxy Error.", 
                    "Could not get voms proxy cert for '%s' (VO: %s), got: %s" %
                    (DN, vo_name, error))


    def check_create_myproxy(self, DN, passwd, myproxy_file):
        """ Checks wether myproxy_file exists. If not it will try to 
            fetch a myproxy from the server. If the myproxy_file exits
            it will check whether it is still valid for at least MIN_VALID_HOURS. 

            raises MyProxyError, MyProxyInputError, MyProxySSLError, ProxyLoadError
        """
        if os.path.exists(myproxy_file) and os.path.isfile(myproxy_file):
            try:
                x509 = X509.load_cert(myproxy_file, X509.FORMAT_PEM)
                enddate = x509.get_not_after().__str__()
            except Exception, e:
                raise ProxyLoadError('Loading Error', 
                    "Loading my_proxy '%s' got '%s'" % (myproxy_file, e))
         
            enddate_tuple = time.strptime(enddate,'%b %d %H:%M:%S %Y GMT')
            enddate_epoch = calendar.timegm(enddate_tuple)
            remaining_hours = (enddate_epoch - int(time.time()))/3600
            
            self.log.debug("Myproxy remainig hours: %d " % remaining_hours)
            self.log.debug("Myproxy minimal  hours: %d " % self.min_myproxy_hours)
            
            if remaining_hours > self.min_myproxy_hours:
                return 
        self.myproxy.myproxy_logon(DN.encode('utf-8'), passwd, myproxy_file)
        os.chmod(myproxy_file, stat.S_IRUSR | stat.S_IWUSR) # else voms-proxy complains
        self.log.debug("Myproxy renewed")




