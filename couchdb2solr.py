from couchdb import CouchDB
from solr import Solr
import lxml.etree

import settings


db = CouchDB(settings.COUCH_HOST, settings.COUCH_PORT )
solr = Solr(settings.SOLR_HOST, settings.SOLR_PORT, settings.SOLR_INDEX)


def create_solr_xml_update(record_list):
    
    add_node =  lxml.etree.Element('add')
    
    for record in record_list['rows']:
        id = record['id']
        doc = record['doc']
        
        if not id.startswith('_design'):        
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
        
        
    return lxml.etree.tostring(add_node, xml_declaration=True, encoding='utf-8')


def index_documents(update_xml, from_doc):
    status, reason, result = solr.index_doc(update_xml)
    if status == 200:
        print "Indexed from %s ==> total of %i documents" % (from_doc, update_xml.count('<doc>'))
    else:
        print "Fail of indexing from %s, reason %s" % (from_doc, reason)
    
    
def commit():
    # commit changes to index 
    status, reason, result = solr.commit()
    if status == 200:
        print "Commit to index OK"
    else:
        print "Fail of commit changes %s" % reason


def main():    
    db_info = db.info_db(settings.DB_NAME)
    steps = (int(db_info['doc_count']) / settings.MAX_DOCS_LIMIT) + 1
    
    for step in range(steps):
        from_doc = step * settings.MAX_DOCS_LIMIT        
        record_list = db.list_docs(settings.DB_NAME, skip=from_doc, limit=settings.MAX_DOCS_LIMIT)
        solr_xml_update = create_solr_xml_update(record_list)
        index_documents(solr_xml_update, from_doc)
    
    commit()

if __name__ == '__main__':
    main()
    
