#!/usr/bin/env python
from M2Crypto import RSA
import binascii

class RSACipher():
    
    def __init__(self, privkey, pubkey=None):
        self.priv_key = RSA.load_key(privkey)
        if pubkey:
            self.pub_key = RSA.load_pub_key(pubkey)

    def private_decrypt(self, ciphertext):
        encrypted = binascii.a2b_hex(ciphertext)
        decrypted = self.priv_key.private_decrypt(encrypted, RSA.pkcs1_padding)
        return decrypted

    def priv_public_encrypt(self, data):
        _data = data.encode('utf-8')
        encrypted =  self.priv_key.public_encrypt(_data, RSA.pkcs1_padding)
        return binascii.b2a_hex(encrypted)

    def public_encrypt(self, data):
        encrypted =  self.pub_key.public_encrypt(data, RSA.pkcs1_padding)
        return binascii.b2a_hex(encrypted)

    def public_decrypt(self, ciphertext):
        encrypted = binascii.a2b_hex(ciphertext)
        decrypted = self.pub_key.public_decrypt(encrypted, RSA.pkcs1_padding)
        return decrypted

if __name__ == '__main__':
    
    ndata = u'lap1ns1'
    rc = RSACipher('/etc/grid-security/hostkey_root.pem') 
    encdata = rc.priv_public_encrypt(ndata)
    fn = 'gugus.txt'
    t = open(fn,'wb')
    t.write(encdata)
    print encdata
    t.close()
    del(rc)
    rc = RSACipher('/etc/grid-security/hostkey_root.pem') 
    t = open(fn,'rb')    
    encdata = t.readline()
    decdata = rc.private_decrypt(encdata)
    print ">%r<" % decdata
    assert ndata == decdata , 'decrypted data matches not original data'
    t.close()    
