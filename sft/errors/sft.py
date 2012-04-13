"""
Container for SFT related errors.
"""

class SFTError(Exception):
    """ Generic exception raised by 
        Site Functional Test 'problems'.
    """
    
    def __init__(self, expr, msg):
        self.expression = expr
        self.message  = msg
    
    def __str__(self):
        return self.message


class SFTInvalidExecTime(SFTError):
    """ Raised for invalid execution times of site 
        functional test.
    """
    pass

class SFTInvalidTestParams(SFTError):
    """ Raised if test parameter, like given clusters,
        associated test users, etc. are not valid.
    """
    pass

class SFTConfigError(SFTError):
    """ Raised when configuration errors, like wrong
        paths, are encountered.
    """
    pass

class SFTMyProxy(SFTError):
    """ Raised for issues while trying to get
        av myproxy user certificate.
    """
    pass

class SFTVomsProxy(SFTError):
    """ Raised for issues while trying to get
        av vomsroxy user certificate.
    """
    pass
