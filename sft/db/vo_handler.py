"""
Dealing with VOs and VO groups
"""

import logging
import sft_meta
import sft_schema as schema
from sqlalchemy import orm

class VOPool():
    
    def __init__(self):
        self.log = logging.getLogger(__name__)
        Session=orm.scoped_session(sft_meta.Session)
        self.session = Session()
        self.log.debug("Initialization finished")


    def add_vo(self, name, server=None):
        vo = self.session.query(schema.VO).filter_by(name=name).first()
        if vo:
            self.log.info("VO '%s' exists already" % name)
            if server and vo.server != server:
                vo.server = server
                self.session.flush()
        else:
            self.log.info("Adding vo '%s'." % name)
            vo = schema.VO(name, server)
            self.session.save(vo)
            self.session.flush()
        self.session.commit() 


    def remove_vo(self, name):
        vo = self.session.query(schema.VO).filter_by(name=name).first()
        if vo:
            self.log.info("Removing vo '%s'." % name)
            self.session.delete(vo)   
            self.session.flush()
            self.session.commit()
            self.session.clear() # -> make sure things get reloaded freshly
    

class VOGroupPool():
        
    def __init__(self):
        self.log = logging.getLogger(__name__)
        Session=orm.scoped_session(sft_meta.Session)
        self.session = Session()
        self.log.debug("Initialization finished")

    def create_group(self, groupname):
        group = self.session.query(schema.VOGroup).filter_by(name=groupname).first()
        if group:
            self.log.info("VO group '%s' exists already" % groupname)
        else:
            self.session.save(schema.VOGroup(groupname))
            self.session.flush()
            self.session.commit()
            self.session.clear() # -> make sure things get reloaded freshly


    def remove_group(self, groupname):
        group = self.session.query(schema.VOGroup).filter_by(name=groupname).first()
        if group:
            self.log.info("Removing group '%s'." % groupname)
            self.session.delete(group)
            self.session.flush()
            self.session.commit()
            self.session.clear() # -> make sure things get reloaded freshly
             

    def add_vo(self,groupname,voname):
        """ will create group if it doesn't exist. """
        group = self.session.query(schema.VOGroup).filter_by(name=groupname).first()
        vo = self.session.query(schema.VO).filter_by(name=voname).first()
        
        if not vo:
            self.log.warn("VO '%s' does not exist. Can't add it to group '%s'." % (voname, groupname))
            return

        if not group:
            self.log.info("Group '%s' does not exist, will be created." % groupname)
            group = schema.VOGroup(groupname)
            self.session.save(group)
        
        if not vo in group.vos:
            group.vos.append(vo) 
        
        self.session.flush()
        self.session.commit()
        self.session.clear() # -> make sure things get reloaded freshly
    
class VOUserPool():
    
    def __init__(self):
        self.log = logging.getLogger(__name__)
        Session=orm.scoped_session(sft_meta.Session)
        self.session = Session()
        self.log.debug("Initialization finished")


    def add_user(self,voname,DN):
        """ only adds user if both vo and user already exist. """
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
        
        self.session.flush()
        self.session.commit()
        self.session.clear() # -> make sure things get reloaded freshly

    def remove_user(self,voname,DN):
        vo = self.session.query(schema.VO).filter_by(name=voname).first()
        user = self.session.query(schema.User).filter_by(DN=DN).first()
        
        if not vo or not user:
            return
       
        user.vos.remove(vo) 
        assert  vo not in user.vos ,'user still member of VO'
        
        self.session.flush()
        self.session.commit()
        self.session.clear() # -> make sure things get reloaded freshly


 
