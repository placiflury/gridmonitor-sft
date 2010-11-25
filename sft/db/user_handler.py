"""
Dealing with cluster and cluster groups
"""

import logging
import sft_meta
import sft_schema as schema

class UserPool():
    
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.session=sft_meta.Session()
        self.log.debug("Initialization finished")
    
    def __del__(self):
        self.session.close()

    def add_user(self,DN, pwd):
        user = self.session.query(schema.User).filter_by(DN=DN).first()
        if user:
            self.log.info("User '%s' exists already" % DN)
        else:
            self.log.info("Adding user '%s'." % DN)
            user = schema.User(DN,pwd)
            self.session.add(user)
        self.session.commit() 


    def get_user_passwd(self,DN):
        user = self.session.query(schema.User).filter_by(DN=DN).first()
        if user:
            try:
                passwd = user.get_passwd() 
            except Exception, e:
                passwd = None
                self.log.error("Could not fetch password of user '%s', got '%r'" 
                    % (DN,e))
            finally:
                return passwd
    
    def reset_user_passwd(self,DN,passwd):
        user = self.session.query(schema.User).filter_by(DN=DN).first()
        if user:
            user.reset_passwd(passwd)
            self.session.commit()


    def remove_user(self,DN):
        user = self.session.query(schema.User).filter_by(DN=DN).first()
        if user:
            self.log.info("Removing user '%s'." % DN)
            self.session.delete(user)   
            self.session.commit()
    
