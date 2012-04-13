"""
Container for proxy certificatate related errors.
"""

class ProxyError(Exception):
    """ Generic exception raised by 
        Proxy related  'problems'.
    """
    
    def __init__(self, expr, msg):
        self.expression = expr
        self.message  = msg
    
    def __str__(self):
        return self.message


class ProxyLoadError(ProxyError):
    """ Raised for errors related with loading proxy
        from local file.
    """
    pass

class VomsProxyError(ProxyError):
    """ Raised for errors related with requesting
        a voms proxy certificate.
    """
    pass
