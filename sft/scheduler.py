"""
Scheduler for running and managing everything 
around the SFT tests.
"""
import logging
from datetime import datetime
from housekeeper import Housekeeper
from publisher import Publisher

import sft_globals as g

class Scheduler(object):
    
    def __init__(self):
        self.log = logging.getLogger(__name__)        
        
        self.housekeeper = Housekeeper()
        self.publisher = Publisher()
        self.log.info("Initialization finished")
    
    
    def main(self):

        t = datetime(*datetime.utcnow().timetuple()[:5])
        
        self.housekeeper.main()

        _down_clusters = self.housekeeper.scheduled_down_clusters

        # fetch results of previous SFTs and publish them
        self.publisher.main()

        # run eligible SFTs
        for sft in self.housekeeper.sfts:
            self.log.debug("Checking SFT  '%s' for execution" % sft.get_name())
            sft.set_clusters_down(_down_clusters)
            sft.check_exec(t)


        # notify nagios on stata 
        g.notifier.notify()
