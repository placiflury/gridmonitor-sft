"""
Configuration object for SFT module. Reads and checks the configuration
paramaters that are required by the ch.smscg.sft package.

"""

import os.path
import ConfigParser
import logging

from errors.config import ConfigError
from socket import gethostbyaddr, gethostname
  
class Config(object):
    """ Configuration of SFT module. """

    # options and there default values
    FILE_OPTIONS = {'hostcert_file': '/etc/grid-security/hostcert.pem',
            'hostkey_file': '/etc/grid-security/hostkey_root.pem',
            'private_key' : '/etc/grid-security/hostkey.pem',
            'curl_bin': '/usr/bin/curl'}

    PATH_OPTIONS = {'url_root': '/opt/smscg/monitor/sft',
            'jobsdir': '/opt/smscg/monitor/sft/jobs',
            'arc_client_tools': '/opt/nordugrid/bin',
            'proxy_dir': '/tmp'}

    DB_SFT_OPTIONS = {'sqlalchemy_sft.url': None,
            'sqlalchemy_sft.pool_recycle': 3600}
    DB_NAGIOS_OPTIONS = {
            'sqlalchemy_nagios.url': None,
            'sqlalchemy_nagios.pool_recycle': 3600 } 

    OPTIONS = {'max_jobs_age': 2880,
            'refresh_period': 10,
            'myproxy_server' : 'myproxy.smscg.ch',
            'myproxy_lifetime': 43200,
            'myproxy_port': 7512,
            'min_myproxy_valid_hours' : 10,
            'min_vomsproxy_valid_hours': 2,
            'proxy_type': 'old',
            'proxy_policy': 'normal',
            'nscaweb_host': 'nagios.smscg.ch',
            'nscaweb_port': 5667, 
            'nscaweb_queue': None, 
            'nscaweb_user': None,
            'nscaweb_pwd' : None,
            'public_key' : None,
            'new_private_key' : None,
            'new_public_key' : None,
            'localhost': gethostbyaddr(gethostname())[0]}
    
    CONSTANTS = {}
    
    
    @property
    def localhost(self):
        """ FQDN of local host"""
        return self.__get_option('localhost')
    
    @property
    def nscaweb_host(self):
        """ FQDN of nscaweb host, usually same as where nagios server runs"""
        return self.__get_option('nscaweb_host')

    @property     
    def nscaweb_port(self):
        """ Returns port on which nscaweb daemon (runs on nscaweb_host)
             is listening.
        """
        return self.__get_option('nscaweb_port')
    
    @property
    def nscaweb_queue(self):
        """ Returns nscaweb queue. """
        return self.__get_option('nscaweb_queue')

    @property
    def nscaweb_user(self):
        """ Returns username of user that is allowed
            to insert in nscaweb_queueue """
        return self.__get_option('nscaweb_user')
    
    @property
    def nscaweb_pwd(self):
        """ Returns password in cleartext of 
            nscaweb_user. """ 
        return self.__get_option('nscaweb_pwd')


    @property
    def myproxy_server(self):
        """ FQDN of myproxy server"""
        return self.__get_option('myproxy_server')

    @property
    def myproxy_port(self):
        """ Port of myproxy server """
        return int(self.__get_option('myproxy_port'))
    
    @property
    def proxy_type(self):
        """ Type of myproxy certificate """
        return self.__get_option('proxy_type')

    @property
    def min_myproxy_hours(self):
        """ Minimal hours myproxy must be valid """
        return int(self.__get_option('min_myproxy_valid_hours'))

    @property
    def min_voms_proxy_hours(self):
        """ Minimal hours voms proxy must be valid """
        return int(self.__get_option('min_voms_proxy_valid_hours'))


    @property
    def proxy_policy(self):
        """ Policy of myproxy certificate """
        return self.__get_option('proxy_policy')

    @property
    def arc_clients(self):
        """ Paths to arc client tools, like ngget, arcsub etc. """
        return self.__get_option('arc_client_tools')

    @property
    def proxy_dir(self):
        """ Directoy where to store myproxy credentials. Notice, 
            the credentials will be encrypted with host private key. 
        """
        return self.__get_option('proxy_dir')

    @property
    def sqlalchemy_sft(self):
        """ returns a dictionary that can be 
            direclty passed to sqlalchemy's 
            engine_from_config method.
        """
        return Config.DB_SFT_OPTIONS.copy()
    
    @property
    def sqlalchemy_nagios(self):
        """ returns a dictionary that can be 
            direclty passed to sqlalchemy's 
            engine_from_config method.
        """
        return Config.DB_NAGIOS_OPTIONS.copy()

    @property
    def max_jobs_age(self):
        """ Age threshold for SFT jobs in DB. They get
            removed afterwards.
        """
        return int(self.__get_option('max_jobs_age'))

    @property
    def refresh_period(self):
        """ Period for checking whether the SFT settings
            have changed. Used to feed in changes 
            that were e.g. entered via GridMonitor web-interface. 
        """
        return int(self.__get_option('refresh_period'))

    @property
    def hostcert(self):
        """ Host certificate. """
        return self.__get_option('hostcert_file')

    @property
    def hostkey(self):
        """ Key of host certificate. Must be accessible
            by SFT daemon process (i.e. owned by root.)
        """
        return self.__get_option('hostkey_file')

    @property
    def private_key(self):
        """ Private key, it's the same as the hostkey,
            but owned by apache process. Used by GridMonitor
            to update the user credentials of SFT users.
         """
        return self.__get_option('private_key')
    
    @property
    def new_private_key(self):
        """ New private key, used if old is to be replaced. 
            raises ConfigErro if option set and is pointing to 
            an non-existing file.
        """
        option = 'new_private_key'
        _file =  self.__get_option(option)

        if _file and not os.path.exists(_file) and not os.path.isfile(_file):
            self.log.error("Paramenter '%s' points to non-existing file '%s')" % \
            (option, _file))
            raise ConfigError('File Error',  "Paramenter '%s' points to non-existing file '%s')" % \
                    (option, _file))
        else:
            return None
    
    @property
    def public_key(self):
        """ Public key, used if old is to be replaced. 
            raises ConfigErro if option set and is pointing to 
            an non-existing file.
        """
        option = 'public_key'
        _file =  self.__get_option(option)

        if _file and not os.path.exists(_file) and not os.path.isfile(_file):
            self.log.error("Paramenter '%s' points to non-existing file '%s')" % \
            (option, _file))
            raise ConfigError('File Error',  "Paramenter '%s' points to non-existing file '%s')" % \
                    (option, _file))
        else:
            return None
    
    @property
    def new_public_key(self):
        """ New public key, used if old is to be replaced. 
            raises ConfigErro if option set and is pointing to 
            an non-existing file.
        """

        option = 'new_public_key'
        _file =  self.__get_option(option)

        if _file and not os.path.exists(_file) and not os.path.isfile(_file):
            self.log.error("Paramenter '%s' points to non-existing file '%s')" % \
            (option, _file))
            raise ConfigError('File Error',  "Paramenter '%s' points to non-existing file '%s')" % \
                    (option, _file))
        else:
            return None
    
    @property
    def curl_bin (self):
        """ curl binary file """
        return self.__get_option('curl_bin')

    @property
    def url_root(self):
        """ Directory where 'html'-wrapped references to 
            the SFT jobs will be stored/created. These are
            used by GridMonitor portal to show SFT job      
            result details. 
        """
        return self.__get_option('url_root')

    @property
    def jobsdir(self):
        """ Download directory of for SFT jobs"""
        return self.__get_option('jobsdir')

    def __init__(self, config_file):
        """
            config_file - path to configuration file.

            raises ConfigError for file not found and invalid content
        """
   
        self.log = logging.getLogger(__name__)

        self.parser = ConfigParser.ConfigParser()
        if os.path.exists(config_file) and os.path.isfile(config_file):
            self.parser.read(config_file)
            self.log.debug("opened configuration '%s'" % config_file)
        else:
            raise ConfigError("Config file missing", "File '%s' doesn't exist." % (config_file))

        self.config_file = config_file
        self.check_config()


    def __get(self, option=None):
        """
        Reads options from the [general] section of the config file.

        If no 'option' argument has been passed it will return 
        all options (and values) of the [general] section. 
        If an options has been specified its value, or None if the 
        value does not exist weill be returned.  
        """

        general = self.parser.options('general')

        gen = {}
        if not general:
            if option:
                return None
            return gen

        for item  in general:
            value = self.parser.get('general', item).strip()
            if value:
                gen[item] = value

        if option:
            if gen.has_key(option):
                return gen[option]
            return None
        return gen
    
    def __get_option(self, option):
        """ 
        Helper method. Tries to get  value for given 
        option from configuration file. If there was
        no value defined, it falls back to default 
        values.

        returns value of given option
        """
        if option in Config.OPTIONS.keys():
            _default = Config.OPTIONS[option]
        elif option in Config.FILE_OPTIONS.keys():
            _default = Config.FILE_OPTIONS[option]
        elif option in Config.PATH_OPTIONS.keys():
            _default = Config.PATH_OPTIONS[option]
        else:
            _default = None # XXX ??
        
        _val = self.__get(option)

        if _val: 
            return _val
        else:
            return _default
        

    def check_config(self):
        """
        Reads and checks entire configuration. 

        Currently only existence of paths and files are checked.
        raises ConfigError if checks fail
        """
        cfgs = self.__get() 
        
        for option in Config.FILE_OPTIONS.keys():
            _default = Config.FILE_OPTIONS[option]
            
            if not cfgs.has_key(option):
                self.log.warn("Parameter '%s' is missing in '%s', using default('%s')" % \
                    (option, self.config_file, _default))
                _file = _default
            else:
                _file = cfgs[option]
                Config.FILE_OPTIONS[option] = _file

            if not os.path.exists(_file) and not os.path.isfile(_file):
                self.log.error("Paramenter '%s' points to non-existing file '%s')" % \
                    (option, _file))
                raise ConfigError('File Error',  "Paramenter '%s' points to non-existing file '%s')" % \
                    (option, _file))


        for option in Config.PATH_OPTIONS.keys():
            _default = Config.PATH_OPTIONS[option]
            
            if not cfgs.has_key(option):
                self.log.warn("Parameter '%s' is missing in '%s', using default('%s')" % \
                    (option, self.config_file, _default))
                _dir = _default
            else:
                _dir = cfgs[option]
                Config.PATH_OPTIONS[option] = _dir

            if not os.path.exists(_dir) and not os.path.isdir(_dir):
                self.log.error("Paramenter '%s' points to non-existing directory '%s')" % \
                    (option, _dir))
                raise ConfigError('File Error',  "Paramenter '%s' points to non-existing directory '%s')" % \
                    (option, _dir))

        
        Config.DB_SFT_OPTIONS['sqlalchemy_sft.url'] = cfgs['sqlalchemy_sft.url']
        Config.DB_NAGIOS_OPTIONS['sqlalchemy_nagios.url'] = cfgs['sqlalchemy_nagios.url']

        self.log.debug("Configuration successfully checked")

