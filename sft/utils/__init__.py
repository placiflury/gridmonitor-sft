import config_parser 

def init_config(config_file):
    """ Call me before using settings form configuration file """
    config_parser.config  = config_parser.ConfigReader(config_file)


