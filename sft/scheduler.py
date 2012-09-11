"""
Scheduler for running and managing everything 
around the SFT tests.
"""
import logging
import time
from Queue import Queue, Empty
from threading import Thread

from sft.housekeeper import Housekeeper
from sft.publisher import Publisher
import sft.sft_globals as g

class Scheduler(object):
    """ Scheduler, takes care of running SFTs and cleaning up """
    
    THREAD_LIMIT = 10       # max number of SFT threads
    CYCLE_TIME = 60         # in seconds
    CHECK_INTERVAL = 10     # interval for status check  of submitted SFT jobs 
    
    def __init__(self):
        self.log = logging.getLogger(__name__)        
        
        self.housekeeper = Housekeeper()
        self.publisher = Publisher()
        self.procq = Queue(0)  # no limit to queue,
        self.stop_threads = False
        self.log.info("Initialization finished")
    

    def _run_sfts(self):
        """ runs sfts that have been put
            in processing queue """

        while not self.stop_threads:
            try:
                sft, insert_time = self.procq.get(True, 30) # blocking for 30 secs than check stop event
                self.log.debug("Current queueing time: %s seconds" % (time.time() - insert_time))
                sft.set_clusters_down(self.housekeeper.scheduled_down_clusters)
                sft.run()
            except Empty:
                continue
            except Exception, ex:
                self.log.error("Got exception '%r'" % ex)
                 

    def start(self):
        """ starting scheduler """
    
        self.stop_threads = False
        cycle = 1
       
        for _ in xrange(Scheduler.THREAD_LIMIT):
            tr = Thread(target=self._run_sfts)
            tr.start()


        while True:
            try:
                timestamp = time.time()
                self.housekeeper.main()
        
                # fetch results of previous SFTs and publish them
                cycle -= 1
                if cycle <= 1:
                    self.publisher.main()
                    cycle = Scheduler.CHECK_INTERVAL

                # add  eligible SFTs to processing queue
                for sft in self.housekeeper.sfts:
                    self.log.debug("Checking SFT '%s' for execution." % sft.get_name())
                    if sft.is_exec_time():
                        self.log.debug("Staging SFT '%s' for execution." % sft.get_name())
                        self.procq.put((sft, timestamp))

                # notify nagios on stata 
                g.notifier.notify()

                # wait  before next turn
                proctime = time.time() - timestamp
                if proctime > Scheduler.CYCLE_TIME:
                    continue
                else:
                    time.sleep(Scheduler.CYCLE_TIME - proctime)

            except Exception, e:
                self.log.error("Got execption %r", e)
                time.sleep(Scheduler.CYCLE_TIME - proctime)
                

    def stop(self):
        """ stopping scheduler """
        self.stop_threads = True

