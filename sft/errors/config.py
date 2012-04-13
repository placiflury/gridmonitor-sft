"""
Container for Configuration  related errors.
"""

class ConfigError(Exception):
    """ Generic exception raised by 
        configuration errors.
    """
    
    def __init__(self, expr, msg):
        self.expression = expr
        self.message  = msg
    
    def __str__(self):
        return self.message
