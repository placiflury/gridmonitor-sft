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
