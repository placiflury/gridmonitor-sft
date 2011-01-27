"""
Dealing with VOs and VO groups
"""

import logging
import sft_meta
import sft_schema as schema
from sft.utils.helpers import strip_args

class VOPool():
    """ Pool of VOs that can be used to create VO groups (see VOUserPool),
        which are themselves used to create Site Functional Tests.
    """

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.session = sft_meta.Session()
        self.log.debug("Initialization finished")

    @strip_args
    def add_vo(self, name, server=None):
        """ Adding a VO to pool of VOs. 
            params: name - name of VO 
                    server - DN of server that hosts VO e.g. voms.smscg.ch
        """
                    
        vo = self.session.query(schema.VO).filter_by(name=name).first()
        if vo:
            self.log.info("VO '%s' exists already" % name)
            if server and vo.server != server:
                vo.server = server
        else:
            self.log.info("Adding vo '%s'." % name)
            vo = schema.VO(name, server)
            self.session.add(vo)
        self.session.commit() 

    @strip_args
    def remove_vo(self, name):
        """ Removing VO from pool of VOs.
            params: name - name of VO
        """
        vo = self.session.query(schema.VO).filter_by(name=name).first()
        if vo:
            self.log.info("Removing vo '%s'." % name)
            self.session.delete(vo)   
            self.session.commit()

    def list_vos(self): 
        """ listing of existing VOs in global 'VO' pool 
            returns: list of VO objects
        """
        return self.session.query(schema.VO).all()


class VOGroupPool():
    """ Class to build VO groups, which can then be specified to 
            be included for SFTs.
    """

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.session = sft_meta.Session()
        self.log.debug("Initialization finished")

    @strip_args
    def create_group(self, groupname):
        """ Creates a VO group:
            params: groupname - name of the VO group
        """
        group = self.session.query(schema.VOGroup).filter_by(name=groupname).first()
        if group:
            self.log.info("VO group '%s' exists already" % groupname)
        else:
            self.session.add(schema.VOGroup(groupname))
            self.log.info("Created VO group '%s'." % groupname)
            self.session.commit()
    
    @strip_args
    def remove_group(self, groupname):
        """ Removes existing VO group. Notice, VOs of global pool
            aren't removed, it's the group that is removed. 
            params: groupname - name of group to remove.
        """
        group = self.session.query(schema.VOGroup).filter_by(name=groupname).first()
        if group:
            self.log.info("Removing group '%s'." % groupname)
            self.session.delete(group)
            self.session.commit()
    
    @strip_args
    def add_vo(self,groupname,voname):
        """ Adding a VO to a VO group. If VO group
            doesn't exist yet, it will be created.
            params: groupname - name of VO group
                    voname - name of VO
        """
        group = self.session.query(schema.VOGroup).filter_by(name=groupname).first()
        vo = self.session.query(schema.VO).filter_by(name=voname).first()

        if not vo:
            self.log.warn("VO '%s' does not exist. Can't add it to group '%s'." % (voname, groupname))
            return

        if not group:
            self.log.info("Group '%s' does not exist, will be created." % groupname)
            group = schema.VOGroup(groupname)
            self.session.add(group)
        
        if not vo in group.vos:
            group.vos.append(vo) 
        
        self.session.commit()
    
    @strip_args 
    def remove_vo(self, groupname, voname):
        """ Removing VO from group.
            params: groupname - name of group
                    VO - name of VO to remove from group.
        """
        group = self.session.query(schema.VOGroup).filter_by(name=groupname).first()
        vo = self.session.query(schema.VO).filter_by(name=voname).first()
        
        if group and vo in group.vos:
            group.vos.remove(group)
            self.log.debug("Removed VO '%s' from VO group '%s'." % (voname, groupname))
            self.session.commit()

    @strip_args
    def list_vos(self, groupname):
        """ Listing of VOs of given group. 
            params: groupname - name of VO group
            returns: list of VO objects or None (eg. if group doesn't exist)
        """
        group = self.session.query(schema.VOGroup).filter_by(name=groupname).first()
        if not group or not group.vos:
            return None

        return group.vos
    
    def list_groups(self):
        """ Listing all existing VO groups.
            return list of VOGroup objects.
        """
        return self.session.query(schema.VOGroup).all()


class VOUserPool():
    """ Class that allows assignment of Users to a specific VO. """
    
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.session=sft_meta.Session()
        self.log.debug("Initialization finished")

    @strip_args
    def add_user(self,voname,DN):
        """ Adding a user to a VO. User will only be added if both
            the VO and the user exist.
            params: voname - name of the VO
                    DN - X509 DN of user
        """
        vo = self.session.query(schema.VO).filter_by(name=voname).first()
        user = self.session.query(schema.User).filter_by(DN=DN).first()
        
        if not vo:
            self.log.warn("VO '%s' does not exist. Can't add users." % voname)
            return

        if not user:
            self.log.warn("User '%s' does not exist. Can't add it to VO '%s'." % (DN, voname))
            return
        
        if not vo in user.vos:
            user.vos.append(vo) 
            self.session.commit()

    @strip_args
    def remove_user(self,voname,DN):
        """ Removing a user from VO. 
            params: voname - name of VO
                    DN - X509 DN of user
        """
        vo = self.session.query(schema.VO).filter_by(name=voname).first()
        user = self.session.query(schema.User).filter_by(DN=DN).first()
    
        
        if vo and user in vo.users:
            vo.users.remove(user)
            self.log.debug("User '%s' removed from VO '%s'." % (DN, voname))
            self.session.commit()
    
    @strip_args
    def list_users(self,voname):
        """ Listing users, which have been associate with VO.
            params: voname - name of VO 
            returns: list of User objects, or None (eg. if VO does not exist)
        """
        vo = self.session.query(schema.VO).filter_by(name=voname).first()
        if not vo or not vo.users:
            return None
        return vo.users

    def list_vos(self):
        """ Listing all VO groups. 
            returns list of VO group objects.
        """
        return self.session.query(schema.VO).all()
 
