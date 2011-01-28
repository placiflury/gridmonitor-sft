#!/usr/bin/env python
""" Helper functions and decorators """

def strip_args(func):
    """ Decorator that strips the input parameters 
        of a function. If arg is a string
        it gets explicitly passed to the function 
        as 'unicode'.
    """
    def new_func(*args, **kwargs):
        args_stripped = list()
        kwargs_stripped = dict()

        for arg in args:
            if type(arg) == str:
                args_stripped.append(unicode(arg.strip()))
            else:
                args_stripped.append(arg)

        if kwargs:
            for k, v in kwargs.items():
                if type(v) == str:
                    kwargs_stripped[k] = unicode(v.strip())
                else:
                    kwargs_stripped[k] = v

        return func(*args_stripped, **kwargs_stripped)
    
    return new_func

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
 
def parse_cron_entry(cron_str, max):
    """
    Parser for *simple* crontab style entries. 
    input: cron_str - crontab like entry as string
             covered cases:
             - integer up to max
             - lists  '1,2,3' 
             - ranges  '1-3'
             - steps   '*/4' or '1-4/2'
             - mix list and range '1,2,3,8-12'
            max  - maximal allowed value (type integer), must be > 0. Values 
                   greater then max, will be 'reset' to max.
    returns: list of integers (time entries)

    raises: CronRangeError - for entries exceeding valid (time) range
            CronSyntaxError - for syntax errors
            CronError - else
    """
    if type(max) != int:
        raise CronError("Invalid Max range", 
            "Variable 'max' must be of integer type")     
    if max <= 0:
        raise CronRangeError("Invalid Max value", 
            "Variable 'max' must be > 0")     
 
    if cron_str.isdigit():
        v = int(cron_str)
        if v > max:
            return max
            """
            raise CronRangeError("Exceeding max range", 
                    "'%s' is exceeding max time value '%d'." % (cront_str, max))
            """
        return v

    if '/' in cron_str: # step
        pre = cron_str.split('/')[0]
        try:
            step = int(cron_str.split('/')[1])
        except:
            raise CronSyntaxError("Syntax Error",
                "'%s' in '%s' must be an integer." % (cron_str.spli('/')[1], cron_str))
        
        if pre == '*':
            return range(0, max+1, step)

        if '-' in pre:
            try:
                start = int(pre.split('-')[0])        
                end = int(pre.split('-')[1])
                if end > max:
                    end = max
            except:
                raise CronSyntaxError("Syntax Error","'%s' in '%s' must contain an integer." % (pre, cron_str))
            return range(start, end+1, step)
       
    if ('-' in cron_str) and (',' in cron_str):
        res = list()
        for i in cron_str.split(','):
            v = parse_cron_entry(i, max) 
            if type(v) is int:
                res.append(v)
            else:
                res += v
        if res:
            return res

    if ',' in cron_str:
        res = list()
        for i in cron_str.split(','):
            if i.isdigit():
                v = int(i)
                if v > max:
                    res.append(max)
                else: 
                    res.append(v)
            else:
                raise CronSyntaxError("Syntax Error","'%s' of '%s' is not a valid entry." % (i, cron_str))
        return res
    
    if '-' in cron_str:
        start = int(cron_str.split('-')[0])        
        end = int(cron_str.split('-')[1])
        if end > max:
            end = max
        return range(start, end+1, 1)

    if '*' == cron_str:
        return range(0, max+1, 1)

    raise CronSyntaxError("Unknown/Invalid syntax", 
            "The cron entry '%s' seems not be a valid crontab style entry!" % (cron_str))


if __name__ == '__main__':
    
    # some quick helpers testing

    # 1. normal 
    
    print 'testing: 1,2,4  max: 4'
    print parse_cron_entry('1,2,4', 4)
    print 'testing: 3-12  max: 11'
    print parse_cron_entry('3-12', 11)
    print 'testing: */4  max:13'
    print parse_cron_entry('*/4', 13)
    print 'testing: *  max: 7'
    print parse_cron_entry('*', 7)
    print 'testing: 1-4/2 max:13'
    print parse_cron_entry('1-4/2', 13)
    print 'testing: 1,2,3,8-10  max:13'
    print parse_cron_entry('1,2,3,8-10', 13)
    
    # 2. failing 
    try:
        print 'max: 0'
        print parse_cron_entry('1,2,3', 0)
    except Exception, e:
        print 'Error: ', e.msg
    try:
        print '1,a,3 max:5'
        print parse_cron_entry('1,a,3', 5)
    except Exception, e2:
        print 'Error: ', e2.msg
        
    try:
        print 'testing: 1-5,*/12 max: 59'
        print parse_cron_entry('1-5,*/12', 2)
    except Exception, e3:
        print 'Error: ',e3.msg

