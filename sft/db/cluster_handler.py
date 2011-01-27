"""
Dealing with cluster and cluster groups
"""

import logging
import sft_meta
import sft_schema as schema
from sft.utils.helpers import strip_args

class ClusterPool():
    """ This class is used to define a 'global' set of 
        clusters. From this set, or global pool of clusters,
        groups of clusters can be defined. This groups are typically
        used to define the clusters on which a specific site functional
        test shall be carried out.
    """
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.session = sft_meta.Session()
        self.log.debug("Initialization finished")
    
    def __del__(self):
        if self.session:
            self.session.close()

    @strip_args
    def add_cluster(self, hostname, alias=None):
        """ Adding a cluster to the pool.
            params: hostname - DN name of the cluster
                    alias   - any alias name for cluster
        """
        cluster = self.session.query(schema.Cluster).filter_by(hostname=hostname).first()
        if cluster:
            self.log.info("Cluster '%s' exists already" % hostname)
            if cluster.alias != alias:
                cluster.alias = alias
        else:
            self.log.info("Adding cluster '%s'." % hostname)
            cluster = schema.Cluster(hostname,alias)
            self.session.add(cluster)
        self.session.commit() 

    @strip_args
    def remove_cluster(self, hostname):
        """ Removing a cluster from the cluster pool. 
            params: hostname - the DN name of the cluster to remove
        """
        cluster = self.session.query(schema.Cluster).filter_by(hostname=hostname).first()
        if cluster:
            self.log.info("Removing cluster '%s'." % hostname)
            self.session.delete(cluster)   
            self.session.commit()

    def list_clusters(self):
        """ listing of all clusters which have been defined """
        return self.session.query(schema.Cluster).all()
    

class ClusterGroupPool():
    """ The pool of cluster groups contains the set of all cluster 
        groups which have been defined. 
    """
        
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.session = sft_meta.Session()
        self.log.debug("Initialization finished")

    def __del__(self):
        self.session.close()

    @strip_args
    def create_group(self, groupname):
        """ Creates a cluster group 
            params: groupname - name of the group to add
        """
        group = self.session.query(schema.ClusterGroup).filter_by(name=groupname).first()
        if group:
            self.log.info("Cluster group '%s' exists already" % groupname)
        else:
            self.log.info("Adding group '%s'." % groupname)
            self.session.add(schema.ClusterGroup(groupname))
            self.session.commit()

    @strip_args
    def remove_group(self, groupname):
        """ Removes a cluster group.
            params: groupname - name of group to remove  """
        group = self.session.query(schema.ClusterGroup).filter_by(name=groupname).first()
        if group:
            self.log.info("Removing group '%s'." % groupname)
            self.session.delete(group)
            self.session.commit()

             
    @strip_args 
    def add_cluster(self, groupname, clustername):
        """ Adding a cluster to a cluster group. Will create group if it doesn't exist. 
            params: groupname - name of group where cluster will be added
                    clustername - hostname of cluster to be added
        """
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
        self.session.commit()
    
    @strip_args 
    def remove_cluster(self, groupname, clustername):
        """ Removing cluster from group.
            params: groupname - name of group
                    cluster - name of cluster to remove from group.
        """
        group = self.session.query(schema.ClusterGroup).filter_by(name=groupname).first()
        cluster = self.session.query(schema.Cluster).filter_by(hostname=clustername).first()
        
        if group and cluster in group.clusters:
            group.clusters.remove(cluster)
            self.log.debug("Removing cluster '%s' from group '%s'." % (clustername, groupname))
            self.session.commit()

    @strip_args
    def list_clusters(self, groupname):
        """ Listing all clusters of specified groups. 
            params: groupname - name of cluster group to list
            returns: list of cluster objects, if group exists and isn't empty, else None
        """
        group = self.session.query(schema.ClusterGroup).filter_by(name=groupname).first()
        
        if not group or not group.clusters:
            return None
        
        return group.clusters

    def list_groups(self):
        """ Listing of cluster groups.
            return list of cluster group objects.
        """
        return self.session.query(schema.ClusterGroup).all()
         
