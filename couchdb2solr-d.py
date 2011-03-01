import json, urllib

from zope.interface import implements
from twisted.internet.defer import succeed
from twisted.web.iweb import IBodyProducer

from twisted.internet import reactor
from twisted.web import client
from twisted.web.http_headers import Headers

import lxml.etree
import settings

class StringProducer(object):
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


def fetch_prepare(max_docs):
    skip  = 0
    steps = (max_docs/settings.MAX_DOCS_LIMIT) + 1    
    
    for i in range(steps):        
        reactor.callLater(0.1 * i, documents_list, skip, settings.MAX_DOCS_LIMIT)
        skip = skip + settings.MAX_DOCS_LIMIT
    

def documents_list(skip, limit):    
    get_doc_url = ''.join(['http://',settings.COUCH_HOST,':',settings.COUCH_PORT,'/', settings.DB_NAME, '/', '_all_docs?include_docs=true&skip=', str(skip),'&limit=',str(limit)])

    deferred = client.getPage(get_doc_url)
    deferred.addCallback(documents_listed).addErrback(error, 'documents_list')

def documents_listed(result):
    for i, doc in enumerate(json.loads(result)['rows']):
        reactor.callLater(0.1 * i, document_index, doc)
        

def document_index(record):
    id = record['id']
    if not id.startswith('_design'):
        
        URL = ''.join(['http://', settings.SOLR_HOST,':',settings.SOLR_PORT,'/',settings.SOLR_INDEX,'/update'])
        solr_update_xml = create_solr_xml_update(record)
        agent = client.Agent(reactor)        
        deferred = agent.request(
            uri=URL,
            method='POST',
            bodyProducer=StringProducer(solr_update_xml),
            headers=Headers({'Content-Type':['text/xml; charset=utf-8']})
        )
        
        deferred.addCallback(document_indexed, id).addErrback(error, 'document_downloaded')

def document_indexed(result, id):
    print "Indexed: %s" % id

def error(error, function):
    print '(%s) Ops!, %s' % (function, error)
    reactor.stop()

def create_solr_xml_update(record):

    doc = record['doc']
    add_node =  lxml.etree.Element('add')
    doc_node = lxml.etree.SubElement(add_node, 'doc')
    
    for field_name in doc:
        field_value = doc[field_name]
        if type(field_value) is not list:
            field_node = lxml.etree.SubElement(doc_node, 'field', attrib={'name':field_name}) 
            field_node.text = field_value
        else:
            for list_value in field_value:
                field_node = lxml.etree.SubElement(doc_node, 'field', attrib={'name':field_name}) 
                field_node.text = list_value
    
    return lxml.etree.tostring(add_node, xml_declaration=True, encoding='utf-8')

def main():
    
    info_db_url = ''.join(['http://',settings.COUCH_HOST, ':',settings.COUCH_PORT,'/', settings.DB_NAME,'/' ])
    db_info = json.load(urllib.urlopen(info_db_url))
    doc_count = int(db_info['doc_count'])
    
    reactor.callLater(0, fetch_prepare, doc_count)
    print 'reactor start'
    reactor.run()
    print 'reactor stop'

if __name__ == '__main__':
    main()
