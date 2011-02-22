#! /usr/bin/python2.6

import httplib, json

class CouchDB:
    """Basic wrapper class for operations on a couchDB"""

    def __init__(self, host, port=5984, options=None):
        self.host = host
        self.port = port

    def connect(self):
        return httplib.HTTPConnection(self.host, self.port) # No close()

    # Database operations

    def create_db(self, dbName):
        """Creates a new database on the server"""

        result = self.put(''.join(['/',dbName,'/']), "")
        return json.loads(result.read())

    def delete_db(self, dbName):
        """Deletes the database on the server"""

        result = self.delete(''.join(['/',dbName,'/']))
        return json.loads(result.read())

    def list_db(self):
        """List the databases on the server"""
        result = self.get('/_all_dbs')
        return json.loads(result.read())
        

    def info_db(self, dbName):
        """Returns info about the couchDB"""
        result = self.get(''.join(['/', dbName, '/']))
        return json.loads(result.read())

    # Document operations

    def list_docs(self, dbName, skip=0, limit=100):
        """List all documents in a given database"""

        result = self.get(''.join(['/', dbName, '/', '_all_docs?include_docs=true&skip=' + str(skip) + '&limit=' + str(limit)]))
        return json.loads(result.read())
        

    def list_view(self, dbName, designName, listName, viewName, viewParams, skip=0, limit=100):
        """List all documents in a given database using list and view """

        
        result = self.get(''.join(['/', dbName, '/_design/', designName,'/_list/', listName,'/', viewName, '?', viewParams, '&skip=' + str(skip) + '&limit=' + str(limit)]))
        return result.read()

    def get_doc(self, dbName, docId):
        """Open a document in a given database"""
        result = self.get(''.join(['/', dbName, '/', docId,]))
        return json.loads(result.read())

    def get_doc_view(self, dbName, designName, viewName, docId ):
        """Open a document in a given database"""
        result = self.get(''.join(['/', dbName, '/_design/', designName,'/_show/', viewName, '/', docId,]))        
        
        return result.read()

    def save_doc(self, dbName, body, docId=None):
        """Save/create a document to/in a given database"""
        if docId:
            result = self.put(''.join(['/', dbName, '/', docId]), body)
        else:
            result = self.post(''.join(['/', dbName, '/']), body)
        return json.loads(result.read())

    def deleteDoc(self, dbName, docId):
        # XXX Crashed if resource is non-existent; not so for DELETE on db. Bug?
        # XXX Does not work any more, on has to specify an revid
        #     Either do html head to get the recten revid or provide it as parameter
        result = self.delete(''.join(['/', dbName, '/', docId, '?revid=', rev_id]))
        return json.loads(result.read())

    # Basic http methods

    def get(self, uri):
        c = self.connect()
        headers = {"Accept": "application/json"}
        c.request("GET", uri, None, headers)
        return c.getresponse()

    def post(self, uri, body):
        c = self.connect()
        headers = {"Content-type": "application/json"}
        c.request('POST', uri, body, headers)
        return c.getresponse()

    def put(self, uri, body):
        c = self.connect()
        if len(body) > 0:
            headers = {"Content-type": "application/json"}
            c.request("PUT", uri, body, headers)
        else:
            c.request("PUT", uri, body)
        return c.getresponse()

    def delete(self, uri):
        c = self.connect()
        c.request("DELETE", uri)
        return c.getresponse()

def prettyPrint(s):
    """Prettyprints the json response of an HTTPResponse object"""

    # HTTPResponse instance -> Python object -> str
    print simplejson.dumps(json.loads(s.read()), sort_keys=True, indent=4)
