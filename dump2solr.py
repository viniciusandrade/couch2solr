from couchdb import CouchDB
from solr import Solr
import lxml.etree
from threading import Thread

import settings

db = CouchDB(settings.COUCH_HOST, settings.COUCH_PORT )
solr = Solr(settings.SOLR_HOST, settings.SOLR_PORT, settings.SOLR_INDEX)

def create_solr_xml_update(record_list):
    
    add_node =  lxml.etree.Element('add')
    
    for record in record_list['rows']:
        id = record['id']
        doc = record['doc']
        
        if not id.startswith('_design'):
            try:
                doc_node = lxml.etree.SubElement(add_node, 'doc')
            
                field_node = lxml.etree.SubElement(doc_node, 'field', attrib={'name':'id'}) 
                field_node.text = id
                for field_name in doc:
                    field_value = doc[field_name]
                    if type(field_value) is not list:
                        field_node = lxml.etree.SubElement(doc_node, 'field', attrib={'name':field_name}) 
                        field_node.text = field_value
                    else:
                        for list_value in field_value:
                            field_node = lxml.etree.SubElement(doc_node, 'field', attrib={'name':field_name}) 
                            field_node.text = list_value
            except:
                print "Unable to convert document id %s" % id 
        
    return lxml.etree.tostring(add_node, xml_declaration=True, encoding='utf-8')


def dump_documents(from_doc):    
    record_list = db.list_docs(settings.DB_NAME, skip=from_doc, limit=settings.MAX_DOCS_LIMIT)
    
    dump_xml = create_solr_xml_update(record_list)
    
    xml_file_name = 'xml/solr-' + str(from_doc) + '.xml'

    xml_file = open(xml_file_name, mode='w')
    
    try:
        write_sucess = xml_file.write(dump_xml)
        print "Dump of %s ==> total of %i documents" % (from_doc, dump_xml.count('<doc>'))
        
    except Exception as detail:
        print "Write dump xml fail ", detail 

    finally:
        xml_file.close()
    
def main():    
    db_info = db.info_db(settings.DB_NAME)
    steps = (int(db_info['doc_count']) / settings.MAX_DOCS_LIMIT) + 1
    
    threads = []
    for step in range(steps):
        from_doc = step * settings.MAX_DOCS_LIMIT        
        #record_list = db.list_docs(settings.DB_NAME, skip=from_doc, limit=settings.MAX_DOCS_LIMIT)
        try:
            threads.append( Thread(target=dump_documents, args=(from_doc,)).start() )
        except:
            print "Error: unable to start thread"

    # Wait for all threads to complete
    for t in threads:
        if t:
            t.join()


if __name__ == '__main__':
    main()
    
