#!/usr/bin/env python 

""" My proxy client. Demo on how to use myproxylib.py """

import logging
import os
import os.path

from myproxylib import MyProxy

import config_parser

__version__ = '0.1'

class MyProxyClient(object):

    def __init__(self, debug = False):
        """ 
        Creates a myproxy client object
        """
        self.log = logging.getLogger(__name__)
        
        self.debug = debug
        self._keyfile = None # used by SSL Context
        self._certfile = None

        self._certfile = config_parser.config.get('hostcert_file')
        self._keyfile = config_parser.config.get('hostkey_file')
        host = config_parser.config.get('myproxy_server')
        _port = config_parser.config.get('myproxy_port')
       
        if not _port:
            port = 7512
        else:
            port = int(_port)

        lifetime = config_parser.config.get('myproxy_lifetime')
        if not lifetime:
            self.lifetime = 43200
        else:
            self.lifetime = int(lifetime)
            
        if not os.path.exists(self._certfile):
            self.log.error("Certificate file '%s' does not exist." % self._certfile)
        if not os.path.exists(self._keyfile):
            self.log.error("Keyfile '%s' does not exist." % self._keyfile)
        
        st = os.stat(self._keyfile)
        user_id = os.geteuid()
        if st.st_uid != user_id: # no handling of that case, may break later ...
            self.log.error("Keyfile '%s' not owned by user runnig process ('%d')" % (self._keyfile, user_id))

        self._my_proxy = MyProxy(host, port)
        self.log.debug("MyProxy: %s:%s with creds: %s %s." % (host, port, self._certfile, self._keyfile))
        

    def set_certfile(self, certfile):
        """ setting certfile for communication with MyProxy server"""
        self._certfile = certfile

    def set_keyfile(self, keyfile):
        """ setting keyfile for communication with MyProxy server"""
        self._keyfile = keyfile
        
    def myproxy_logon(self, username, passphrase, outfile=None):
        """
        Function to retrieve a proxy credential from a MyProxy server
        
        Exceptions:  MyProxyError, MyProxyInputError, MyProxySSLError
        """

        self._my_proxy.init_context(self._certfile, self._keyfile)
        proxy_credential= self._my_proxy.get(username, passphrase)

        if not outfile:
            outfile = '/tmp/x509up_u%s' % (os.getuid())

        print 'Storing proxy in:', outfile

        proxy_credential.store_proxy(outfile)
        


if __name__ == '__main__':
    import optparse
    import getpass

    config_file = "/opt/smscg/sft/etc/config.ini"
     
    config_parser.config =  config_parser.ConfigReader(config_file)


    MIN_PASS_PHRASE = 7 # minimal length of myproxy passphrase
    outfile = None
    usage= "usage: %prog [options] get \n\nDo %prog -h for more help."

    parser = optparse.OptionParser(usage=usage, version ="%prog " + __version__)
    parser.add_option("-l", "--username", dest="username", 
                       help="The username with which the credential is stored on the MyProxy server")
    parser.add_option("-o", "--out", dest="outfile", 
                       help="Filenname under which user proxy certificate gets stored.")
    (options, args) = parser.parse_args()

    if not args:
        parser.error("incorrect number of arguments")

    if args[0] not in ['put', 'get']:
        parser.error("wrong argument")
    
    username = options.username
    while True:
        passphrase = getpass.getpass(prompt="MyProxy passphrase:")
        if len(passphrase) < MIN_PASS_PHRASE:
            print 'Error Passphrase must contain at least %d characters' % MIN_PASS_PHRASE
            continue
        break
        
    try:
        mp = MyProxyClient(debug=True)
        if args[0] == 'get':
            mp.myproxy_logon(username, passphrase, outfile)
            if outfile:
                print "A proxy has been received for user %s in %s." % (username, outfile)
            else:
                 print "A proxy has been received for user %s" % (username)
    except Exception,e:
        print "Error:", e
