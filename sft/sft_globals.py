""" the global variables we're using for the module.
    Notice, the (sqlalchemy) database globals are not
    included here but in the sft.db.sft_meta.py file.
"""

__all__ = ['config', 'notifier', 'pxhandle']

# configuration object, must explicitly initialized in __init__.py
config = None

# notifier module, must also be initialized explicitely in __init__.py
notifier = None

# pxhandle, global dealing with myproxy and vomsproxy issues

pxhandle = None



