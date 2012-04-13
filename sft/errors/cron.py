""" Errors related to cron 'tab' syntax and the like """

class CronError(Exception):
    """ 
    Exception raised for Cron errors.
    Attributes:
        expression -- input expression in which error occurred
        message -- explanation of error 
    """
    def __init__(self, expression, message):
        self.expression = expression
        self.msg = message

class CronRangeError(CronError):
    """ Raised if the specified time is 
        exceeding a max range.
    """
    pass

class CronSyntaxError(CronError):
    """ Raised for syntax errors in cron string """
    pass

