"""
Myproxy and vomsproxy utility clas
"""
__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "11.03.2010"
__version__ = "0.1.0"

import logging
import config_parser
import os, os.path, stat
from M2Crypto import X509
import time, calendar
import myproxy
import subprocess


class ProxyUtil(object):
        
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.last_error_msg  = None
        self.proxy_dir = config_parser.config.get('proxy_dir')
        self.min_vomsproxy_valid_hours = int(config_parser.config.get('min_vomsproxy_valid_hours'))
        self.min_myproxy_valid_hours = int(config_parser.config.get('min_myproxy_valid_hours'))
        self.myproxy = myproxy.MyProxyLogon()
    
    def get_proxy_dir(self):
        """ return proxy directory """
        return self.proxy_dir        

    def check_create_vomsproxy(self, DN, file_prefix, vo_name):
        """
        checks whether proxy for VO exists already and whether it's still valid. if not
        it will try to create a new proxy. 
        returns: True - things went fine
                 False - something went wrong
        """
        ckfile = os.path.join(self.proxy_dir, file_prefix) # key + cert file
        pfile = os.path.join(self.proxy_dir, file_prefix+'_'+vo_name) # proxy file

        if os.path.exists(pfile) and os.path.isfile(pfile):
            try:
                x509 = X509.load_cert(pfile, X509.FORMAT_PEM)
                enddate = x509.get_not_after().__str__()
            except Exception, e:
                self.log.error("Loading voms_proxy '%s' got '%r'" % (pfile, e))
                self.last_error_msg = e.__repr__()
                return False
         
            enddate_tuple = time.strptime(enddate,'%b %d %H:%M:%S %Y GMT')
            enddate_epoch = calendar.timegm(enddate_tuple)
            remaining_hours = (enddate_epoch - int(time.time()))/3600
            self.log.debug("VOMS proxy remainig hours: %d " % remaining_hours)
            self.log.debug("VOMS proxy minimal  hours: %d " % self.min_vomsproxy_valid_hours)
            
            if remaining_hours > self.min_vomsproxy_valid_hours:
                return True
        try:
            cmd ="voms-proxy-init -voms %s -key %s -cert %s -hours 10 -out %s" % (vo_name, ckfile, ckfile, pfile)
            ret = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            ret.poll()
            if ret.returncode == 0:
                self.log.debug("Got voms proxy for %s (VO: %s)" % (DN, vo_name))
                os.chmod(pfile, stat.S_IRUSR | stat.S_IWUSR) # else ngsub complains
                return True
            else:
                # Warnings have return codes != 0, here's a hack to intercept them
                ret_msg = ret.communicate()
                if ('Warning:' in ret_msg[0]) or ('Your proxy is valid until' in ret_msg[0]):
                    self.log.debug("Got voms proxy for %s (VO: %s) with warning." % (DN, vo_name))
                    os.chmod(pfile, stat.S_IRUSR | stat.S_IWUSR) # else ngsub complains
                    return True
         
                #error = ret_msg[1]
                error = ret_msg
                self.log.error("Requesting voms proxy for '%s' (VO: %s), got: %s" %
                    (DN, vo_name, error))
                self.last_error_msg = error
                return False
                
        except Exception, e:
            self.log.error("Could not get myproxy cert for '%s', got '%r'" % (DN, e))
            self.last_error_msg = e.__repr__()
            return False


    def check_create_myproxy(self, DN, passwd, myproxy_file):
        """ Checks wether myproxy_file exists. If not it will try to 
            fetch a myproxy from the server. If the myproxy_file exits
            it will check whether it is still valid for at least MIN_VALID_HOURS. 
            return  True  - things went fine
                    False - something went wrong 
        """
        if os.path.exists(myproxy_file) and os.path.isfile(myproxy_file):
            try:
                x509 = X509.load_cert(myproxy_file, X509.FORMAT_PEM)
                enddate = x509.get_not_after().__str__()
            except Exception, e:
                self.log.error("Loading myproxy '%s' got '%r'" % (myproxy_file, e))
                self.last_error_msg = e.__repr__()
                return False
         
            enddate_tuple = time.strptime(enddate,'%b %d %H:%M:%S %Y GMT')
            enddate_epoch = calendar.timegm(enddate_tuple)
            remaining_hours = (enddate_epoch - int(time.time()))/3600
            
            self.log.debug("Myproxy remainig hours: %d " % remaining_hours)
            self.log.debug("Myproxy minimal  hours: %d " % self.min_myproxy_valid_hours)
            
            if remaining_hours > self.min_myproxy_valid_hours:
                return True
        try:
            self.myproxy.myproxy_logon(DN, passwd, myproxy_file)
            os.chmod(myproxy_file, stat.S_IRUSR | stat.S_IWUSR) # else voms-proxy complains
            return True
        except Exception, e:
            self.log.error("Could not get myproxy cert for '%s', got '%r'" % (DN, e))
            self.last_error_msg = e.__repr__()
            return False
         

    def get_last_error(self):
        return self.last_error_msg
