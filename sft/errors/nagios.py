"""
Container for Nagios Notification  related errors.
"""

class NagiosNotifierError(Exception):
    """ Generic exception raised by 
        Nagios Nofifier  'problems'.
    """
    
    def __init__(self, expr, msg):
        self.expression = expr
        self.message  = msg
    
    def __str__(self):
        return self.message


