"""
Dealing with cluster and cluster groups
"""

import logging
import sft_meta
import sft_schema as schema
from sqlalchemy import orm

class ClusterPool():
    
    def __init__(self):
        self.log = logging.getLogger(__name__)
        Session=orm.scoped_session(sft_meta.Session)
        self.session = Session()
        self.log.debug("Initialization finished")
    
    def __del__(self):
        self.session.close()

    def add_cluster(self,hostname,alias=None):
        cluster = self.session.query(schema.Cluster).filter_by(hostname=hostname).first()
        if cluster:
            self.log.info("Cluster '%s' exists already" % hostname)
            if cluster.alias != alias:
                cluster.alias = alias
                self.session.flush()
        else:
            self.log.info("Adding cluster '%s'." % hostname)
            cluster = schema.Cluster(hostname,alias)
            self.session.add(cluster)
            self.session.flush()
        self.session.commit() 
        #self.session.clear() # -> make sure things get reloaded freshly


    def remove_cluster(self,hostname):
        cluster = self.session.query(schema.Cluster).filter_by(hostname=hostname).first()
        if cluster:
            self.log.info("Removing cluster '%s'." % hostname)
            self.session.delete(cluster)   
            self.session.flush()
            self.session.commit()
            #self.session.clear() # -> make sure things get reloaded freshly
    

class ClusterGroupPool():
        
    def __init__(self):
        self.log = logging.getLogger(__name__)
        Session=orm.scoped_session(sft_meta.Session)
        self.session = Session()
        self.log.debug("Initialization finished")

    def __del__(self):
        self.session.close()

    def create_group(self,groupname):
        group = self.session.query(schema.ClusterGroup).filter_by(name=groupname).first()
        if group:
            self.log.info("Cluster group '%s' exists already" % groupname)
        else:
            self.log.info("Adding group '%s'." % groupname)
            self.session.add(schema.ClusterGroup(groupname))
            self.session.flush()
            self.session.commit()
            #self.session.clear() # -> make sure things get reloaded freshly


    def remove_group(self, groupname):
        group = self.session.query(schema.ClusterGroup).filter_by(name=groupname).first()
        if group:
            self.log.info("Removing group '%s'." % groupname)
            self.session.delete(group)
            self.session.flush()
            self.session.commit()
            #self.session.clear() # -> make sure things get reloaded freshly

             

    def add_cluster(self,groupname,clustername):
        """ will create group if it doesn't exist. """
        group = self.session.query(schema.ClusterGroup).filter_by(name=groupname).first()
        cluster = self.session.query(schema.Cluster).filter_by(hostname=clustername).first()
        
        if not cluster:
            self.log.warn("Cluster '%s' does not exist. Can't add it to group '%s'." % (clustername, groupname))
            return

        if not group:
            self.log.info("Group '%s' does not exist, will be created." % groupname)
            group = schema.ClusterGroup(groupname)
            self.session.add(group)
        
        if not cluster in group.clusters:
            self.log.info("Cluster '%s' added to group '%s'." % (clustername, groupname))
            group.clusters.append(cluster) 
        
        self.session.flush()
        self.session.commit()
        #self.session.clear() # -> make sure things get reloaded freshly
        
