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


def init_nagios_notifier(config):

    """ Call to initialize global notifier variable in 
        nagios_notifier. 
        
        config -- configuration object (as created by init_configuration)
    """
    sft_globals.notifier = nagios_notifier.NagiosNotifier(config)


def init_proxy_util(config):
    """ initialzes global accessible handler to deal
        with myproxy and vomsproxy.
        Notice, must only be called once global configuration
        and notifier objects are  available.

        config -- configuration object
    """
    sft_globals.pxhandle = proxyutil.ProxyUtil(config)
