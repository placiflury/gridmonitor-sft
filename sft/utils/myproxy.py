#!/usr/bin/env python
#
# myproxy client
#
# Adaptation form Tom Uram <turam@mcs.anl.gov> (2005/08/04) myproxy-logon version
#
from __future__ import with_statement 
__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "26.02.2010"
__version__ = "0.1.0"

import logging
import os.path, socket
from OpenSSL import crypto, SSL
import config_parser

class GetException(Exception): 
    pass
class RetrieveProxyException(Exception): 
    pass


class MyProxyLogon(object):
        
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.certfile = config_parser.config.get('hostcert_file')
        self.keyfile = config_parser.config.get('hostkey_file')
        self.myproxy_server = config_parser.config.get('myproxy_server')
        port = config_parser.config.get('myproxy_port')
        if not port:
            self.port = 7512
        else:
            self.port = int(port)
        lifetime = config_parser.config.get('myproxy_lifetime')
        if not lifetime:
            self.lifetime = 43200
        else:
            self.lifetime = int(lifetime)
            
        if not os.path.exists(self.certfile):
            self.log.error("Certificate file '%s' does not exist." % self.certfile)
        if not os.path.exists(self.keyfile):
            self.log.error("Keyfile '%s' does not exist." % self.keyfile)
        
        st = os.stat(self.keyfile)
        user_id = os.geteuid()
        if st.st_uid != user_id: # no handling of that case, may break later ...
            self.log.error("Keyfile '%s' not owned by user runnig process ('%d')" % (self.keyfile, user_id))

    def _create_cert_req(self, keyType = crypto.TYPE_RSA,
                        bits = 1024,
                        messageDigest = "md5"):
        """
        Create certificate request.
    
        Returns: certificate request PEM text, private key PEM text
        """
        crt = crypto.X509Req() # create crt
        pkey = crypto.PKey() # generate private key
        pkey.generate_key(keyType, bits)
        crt.set_pubkey(pkey)
        crt.sign(pkey, messageDigest)
        return (crypto.dump_certificate_request(crypto.FILETYPE_ASN1, crt),
               crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey))
        
    def _deserialize_response(self, msg):
        """
        Deserialize a MyProxy server response
        
        Returns: integer response, errortext (if any)
        """
        lines = msg.split('\n')
        # get response value
        responselines = filter(lambda x: x.startswith('RESPONSE'), lines)
        responseline = responselines[0]
        response = int(responseline.split('=')[1])
        # get error text
        errortext = ""
        errorlines = filter(lambda x: x.startswith('ERROR'), lines)
        for e in errorlines:
            etext = e.split('=')[1]
            errortext += etext
        
        return response, errortext
 
               
    def _deserialize_certs(self, inp_dat):
        pem_certs = []
        dat = inp_dat
        
        while dat:
            # find start of cert, get length        
            ind = dat.find('\x30\x82')
            if ind < 0:
                break
            _len = 256*ord(dat[ind+2]) + ord(dat[ind+3])
            # extract der-format cert, and convert to pem
            c = dat[ind:ind+_len+4]
            x509 = crypto.load_certificate(crypto.FILETYPE_ASN1, c)
            pem_cert = crypto.dump_certificate(crypto.FILETYPE_PEM, x509)
            pem_certs.append(pem_cert)

            # trim cert from data
            dat = dat[ind + _len + 4:]
        return pem_certs



    def myproxy_logon(self, username, passphrase, outfile):
        """
        Function to retrieve a proxy credential from a MyProxy server
        
        Exceptions:  GetException, RetrieveProxyException
        """
        
        cmd = """VERSION=MYPROXYv2\nCOMMAND=0\nUSERNAME=%s\nPASSPHRASE=%s\nLIFETIME=%d\0"""
        context = SSL.Context(SSL.SSLv3_METHOD)
        
        # disable for compatibility with myproxy server (er, globus)
        # globus doesn't handle this case, apparently, and instead
        # chokes in proxy delegation code
        context.set_options(0x00000800L)
        
        context.use_certificate_file(self.certfile)
        context.use_privatekey_file(self.keyfile)
        
        self.log.debug("Connect to myproxy server %s" % self.myproxy_server)
        conn = SSL.Connection(context, socket.socket())
        conn.connect((self.myproxy_server, self.port))
        
        self.log.debug("Sending globus compat byte")
        conn.write('0')

        self.log.debug("Sending globus get command.")
        cmd_get = cmd % (username, passphrase, self.lifetime)
        conn.write(cmd_get)

        self.log.debug("Getting globus response.")
        dat = conn.recv(8192)
        
        self.log.debug("Got: %r" % dat)
        response, errortext = self._deserialize_response(dat)
        if response:
            raise GetException(errortext)
        else:
            self.log.debug("Server response ok")
        
        # generate and send certificate request
        # - The client will generate a public/private key pair and send a 
        #   NULL-terminated PKCS#10 certificate request to the server.
        self.log.debug("Sending cert request.")
        certreq, privatekey = self._create_cert_req()
        conn.send(certreq)

        # process certificates
        # - 1 byte , number of certs
        dat = conn.recv(1)
        numcerts = ord(dat[0])
        
        # - n certs
        self.log.debug("Receiving certs.")
        dat = conn.recv(8192)

        self.log.debug("Getting server response.")
        resp = conn.recv(8192)
        response, errortext = self._deserialize_response(resp)
        if response:
            raise RetrieveProxyException(errortext)
        else:
            self.log.debug("Server response ok")

        pem_certs = self._deserialize_certs(dat)
        if len(pem_certs) != numcerts:
            self.log.warn(" %d certs expected, %d received" % (numcerts, len(pem_certs)))

        self.log.debug("Writing proxy and certs to %s." % outfile)
        with open(outfile,'w') as f:
            f.write(pem_certs[0])
            f.write(privatekey)
            for c in pem_certs[1:]:
                f.write(c)
        
        
    
if __name__ == '__main__':
    import sys
    import optparse
    import getpass
    import logging.config    
    from init import init_config

    init_config('./config/config.ini')
    logging.config.fileConfig("./config/logging.conf")

    parser = optparse.OptionParser()
    parser.add_option("-l", "--username", dest="username", 
                       help="The username with which the credential is stored on the MyProxy server")
    parser.add_option("-o", "--out", dest="outfile", 
                       help="Filenname under which user proxy certificate gets stored.")
    (options, args) = parser.parse_args()
    
    # process options
    username = options.username
    if not username:
        if sys.platform == 'win32':
            username = os.environ["USERNAME"]
        else:
            import pwd
            username = pwd.getpwuid(os.geteuid())[0]
    
    outfile = options.outfile
    if not outfile:
        if sys.platform == 'win32':
            outfile = 'proxy'
        elif sys.platform in ['linux2','darwin']:
            outfile = '/tmp/x509up_u%s' % (os.getuid())

    # Get MyProxy password
    passphrase = getpass.getpass()
        
    # Retrieve proxy cert
    try:
        mp = MyProxyLogon()
        ret = mp.myproxy_logon(username, passphrase, outfile)
        print "A proxy has been received for user %s in %s." % (username, outfile)
    except Exception,e:
        print "Error:", e
    
