#!/usr/bin/env python
"""
Generation of a HTML-index of all files and (sub-) 
directories of a directory. The files get attached a 
'.hmtl' affix, so they can be browsed by a pylons application.
"""
from __future__ import with_statement 
import os.path
import os
import logging

__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "20.04.2010"
__version__ = "0.1.0"


class HTMLIndexerError(Exception):
    """ 
    Exception raised for HTMLIndexer errors.
    Attributes:
        expression -- input expression in which error occurred
        message -- explanation of error 
    """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message



class HTMLIndexer(object):
    """ Creates an index (html) with all files in directory
        passed with 'set_path' method. The files are in addition
        renamed to their original name + '.html' extension. This
        is needed for pylons to be able to display the content of 
        the files.
    """

    def __init__(self, url_root):
        """
        url_root: logical url root, i.e. where apache and the like
                  look for the webcontent. The logical url root must
                  be an existing directory, and must be specified as
                  an absolute path. 
        """
        self.log = logging.getLogger(__name__)

        url_root = url_root.strip()

        if not os.path.exists(url_root):
            self.log.info("url_root ('%s')  does not exist, creating it." % url_root)
            os.mkdir(url_root)
        if not os.path.isabs(url_root):
            raise HTMLIndexerError("URL path error",
                    "url_root ('%s')  must be an absolute path." % url_root)
        if not os.path.isdir(url_root):        
            raise HTMLIndexerError("URL path error", 
                    "url_root ('%s')  must be a directory." % url_root)

        self.url_root = url_root
        self.path = None
        
    def set_path(self, path):
        """
        path: filesystem path. If an absolute path is given, it must
              start with the  url_root path prefix. 
              If a relative path is given, the path will be completed 
              with the url_root prefix. 
        warning: will change permissions to readeable for all
        """
        path = path.strip()

        if os.path.isabs(path):
            if (self.url_root not in path) and \
                (not path[:len(self.url_root)] == self.url_root):
                self.log.error("Absolute  path (%s) must start with url_root (%s)" % \
                    (path, self.url_root))
                raise HTMLIndexerError("Path error", 
                    "Absolute  path (%s) must start with url_root (%s)" % \
                    (path, self.url_root))

        else:
            # remove  ./ ../ ../../ etc. if any
            npath = path.replace('../', '').replace('./','')
            path = os.path.join(self.url_root, npath)
            self.log.debug("Filesystem path set to '%s'" % path)

        if not os.path.exists(path):
            self.log.info("Path %s does not exists, creating it" % path)
            os.mkdir(path)

        self.path = path
        os.chmod(path, 0755)
        self.log.debug("URL root is '%s', and directory to be indexed is '%s'" %  \
            (self.url_root, self.path))


    def get_logical_path(self):
        """ return the logical path, i.e. from url_root on """
        return self.path[len(self.url_root):]
        

    def get_index(self):

        index_list = list() 

        for path, subdir, files in os.walk(self.path):
            os.chmod(path, 0755)
            for oldfile in files:
                oldf = os.path.join(path, oldfile)
                file_size = os.stat(oldf).st_size
                newfile = oldfile + '.html'
                os.rename(oldf, os.path.join(path, newfile))  
                os.chmod(os.path.join(path,newfile), 0644)
                from_root_path = path[len(self.url_root):]
                index_list.append((from_root_path, newfile, file_size))    
        return index_list

    def make_html_index(self, index_list):
        """
        generate html-index and store in 'index.html' file. Notice, if there should be
        an 'index.html' file, it will be renamed by 'get_index()' to 'index.html.html'  
        """
        index_file_name = os.path.join(self.path,'index.html')       
        suffix_len = len('.html')
        cur_path = '' 
        ul_end_flag = False

        with open(index_file_name,'w') as f:
            f.write("<html>\n<head>\n</head>\n<body>\n")        
            for url_path, filename, fsize in index_list:
                if cur_path != url_path:
                    if ul_end_flag: 
                        f.write('</ul>\n')
                        ul_end_flag = False
                    f.write("<b>Files in '%s' </b>\n" % url_path)
                    f.write("<ul>\n")
                    ul_end_flag = True
                    cur_path = url_path
                    
                up = os.path.join(url_path, filename)
                f.write('\t<li> <a href="%s"> %s </a> (%d Bytes) </li>\n' % (up, filename[:-suffix_len], fsize))
            f.write("</ul>\n")
            f.write("</body>\n</hmtl>\n")
 
    def generate(self):
        """ generates hmtl index """
        index = self.get_index()
        self.make_html_index(index)

if __name__ == '__main__':
    import logging.config
    logging.config.fileConfig("./config/logging.conf")

    p = '/opt/GridMonitor/gridmonitor/public/sft/181431271941266454110978'
    root = '/opt/GridMonitor/gridmonitor/public/sft'
    inx = HTMLIndexer(root)
    inx.set_path(p) 
    inx.generate()
