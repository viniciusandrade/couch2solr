#! /usr/bin/python2.6

from urllib import urlopen
from httplib import HTTPConnection


class Solr:
    """Basic wrapper class for operations on a couchDB"""

    def __init__(self, host, port=5984, index=None, options=None):
        self.host = host
        self.port = port
        self.index = index

    def connect(self):
        return httplib.HTTPConnection(self.host, self.port) # No close()

    # Index operations
    
    def index_doc(self, docstr):
        path = '/' + self.index + '/update'

        return self.do(self.host, self.port, 'POST', path, docstr)        
    
    def commit(self):
        path = '/' + self.index + '/update?commit=true'

        return self.do(self.host, self.port, 'GET', path)        

    # Basic http method

    def do(self, host, port, method, path, data=None):
        cnx =  HTTPConnection('%s:%s' % (self.host, self.port))
        if data:
            body = data
            headers = {'Content-type': 'application/json'}
            cnx.request(method, path, body, headers)
        else:    
            cnx.request(method, path)
        res = cnx.getresponse()
        data = res.read()
        cnx.close()
        return (res.status, res.reason, data)
