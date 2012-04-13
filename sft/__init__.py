import configuration
import nagios_notifier
import proxyutil

import sft_globals

def init_configuration(config_file):
    """ 
        config_file -- configuration file of SFT module.
        raises ConfigError in case of problems. 
     """
    sft_globals.config = configuration.Config(config_file)


def init_nagios_notifier(server, 
            send_nsca_cfg = '/etc/send_nsca.cfg', 
            send_nsca_bin = '/usr/sbin/send_nsca'):

    """ Call to initialize global notifier variable in 
        nagios_notifier. 

        server - FQDN of nagios server e.g. nagios.smscg.ch 
        send_nsca_cfg - path to configuration of NSCA client 
        send_nsca_bin - path to NSCA client binary
    """
    sft_globals.notifier = nagios_notifier.NagiosNotifier(server, send_nsca_cfg, send_nsca_bin)


def init_proxy_util(config):
    """ initialzes global accessible handler to deal
        with myproxy and vomsproxy.
        Notice, must only be called once global configuration
        and notifier objects are  available.

        config -- configuration object
    """
    sft_globals.pxhandle = proxyutil.ProxyUtil(config)
