"""
Container for Publisher related errors.
"""

class PublisherError(Exception):
    """ Generic exception raised by 
        Site Functional Publisher 'problems'.
    """
    
    def __init__(self, expr, msg):
        self.expression = expr
        self.message  = msg
    
    def __str__(self):
        return self.message

class PublisherConfigError(PublisherError):
    """ Raised when configuration errors, like wrong
        paths, are encountered.
    """
    pass

class PublisherMyProxy(PublisherError):
    """ Raised for issues while trying to get
        av myproxy user certificate.
    """
    pass

class PublisherVomsProxy(PublisherError):
    """ Raised for issues while trying to get
        av vomsroxy user certificate.
    """
    pass
