"""
Dealing with cluster and cluster groups
"""

import logging
import sft_meta
import sft_schema as schema
from sft.utils.helpers import strip_args

class UserPool():
    """ Creates a 'global' pool of users, which can be 
        assigned to VOs.
    """

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.session=sft_meta.Session()
        self.log.debug("Initialization finished")
    
    def __del__(self):
        self.session.close()

    @strip_args
    def add_user(self, DN, display_name,pwd):
        """ Adding a user to the user pool. 
            params: DN - X509 DN of user
                    display_name - display name for user
                    pwd - password of user, will be stored encrypted 
                        with hostkey of machine where SFTs run.
        """
        user = self.session.query(schema.User).filter_by(DN=DN).first()
        if user:
            self.log.info("User '%s' exists already" % DN)
            if user.display_name != display_name:
                user.display_name = display_name
                self.session.add(user)
        else:
            self.log.info("Adding user '%s'." % DN)
            user = schema.User(DN, display_name, pwd)
            self.session.add(user)
        self.session.commit() 

    @strip_args
    def get_user_passwd(self,DN):
        """ Fetching the password of the user. 
            params: DN - X509 DN of user
            returns: password of user in plaintext
        """
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
    @strip_args 
    def reset_user_passwd(self, DN, pwd):
        """ Resetting user password.
            params: DN - X509 DN of user
                    pwd - new user password. Will be stored encrypted
                        with hostkey of machine where SFTs run
        """
        user = self.session.query(schema.User).filter_by(DN=DN).first()
        if user:
            user.reset_passwd(pwd)
            self.session.commit()

    @strip_args
    def remove_user(self,DN):
        """ Removing user from pool of users. 
            params: DN - X509 DN of user.
        """
        user = self.session.query(schema.User).filter_by(DN=DN).first()
        if user:
            self.log.info("Removing user '%s'." % DN)
            self.session.delete(user)   
            self.session.commit()
    
    def list_users(self):
        """ Listing all users in pool of users. 
            Return - list of user objects. 
        """
        return self.session.query(schema.User).all()
