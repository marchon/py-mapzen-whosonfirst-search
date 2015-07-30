# https://pythonhosted.org/setuptools/setuptools.html#namespace-packages
# __import__('pkg_resources').declare_namespace(__name__)

import os.path
import csv
import geojson
import logging

# https://elasticsearch-py.readthedocs.org/en/master/

import elasticsearch
import elasticsearch.helpers

# wuh...
# https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html

# return stuff that can be passed to:
# https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html

class base:

    def __init__(self, **kwargs):

        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 9200)

        timeout = kwargs.get('timeout', 600)

        es = elasticsearch.Elasticsearch(host=host, port=port, timeout=timeout)
        self.es = es

class index(base):
    
    def __init__(self, **kwargs):
        base.__init__(self, **kwargs)

        self.index = 'whosonfirst'
        self.doctype = None

    def prepare_feature(self, feature):

        props = feature['properties']

        id = props['wof:id']

        doctype = props['wof:placetype']
        body = self.prepare_geojson(feature)

        return {
            'id': id,
            'index': self.index,
            'doc_type': doctype,
            'body': body
        }
    
    # https://stackoverflow.com/questions/20288770/how-to-use-bulk-api-to-store-the-keywords-in-es-by-using-python

    def prepare_feature_bulk(self, feature):

        props = feature['properties']
        id = props['wof:id']
    
        doctype = props['wof:placetype']

        body = self.prepare_geojson(feature)

        return {
            '_id': id,
            '_index': self.index,
            '_type': doctype,
            '_source': body
        }

    def prepare_geojson(self, geojson):

        props = geojson['properties']

        # Store a stringified bounding box so that tools like
        # the spelunker can zoom to extent and stuff like that
        # (20150730/thisisaaronland)

        bbox = geojson.get('bbox', [])
        bbox = map(str, bbox)	# oh python...
        bbox = ",".join(bbox)
        props['geom:bbox'] = bbox

        return props

    def load_file(self, f):
        fh = open(f, 'r')
        return geojson.load(fh)

    def prepare_file(self, f):

        data = self.load_file(f)
        data = self.prepare_feature(data)
        return data

    def prepare_file_bulk(self, f):

        logging.debug("prepare file %s" % f)

        data = self.load_file(f)

        data = self.prepare_feature_bulk(data)
        logging.debug("yield %s" % data)

        return data

    def prepare_files_bulk(self, files):

        for path in files:

            logging.debug("prepare file %s" % path)

            data = self.prepare_file_bulk(path)
            logging.debug("yield %s" % data)

            yield data

    def index_file(self, path):

        path = os.path.abspath(path)
        data = self.prepare_file(path)

        return self.es.index(**data)

    def index_files(self, files):

        iter = self.prepare_files_bulk(files)
        return elasticsearch.helpers.bulk(self.es, iter)

class search(base):

    def __init__(self, **kwargs):

        base.__init__(self, **kwargs)

        self.page = kwargs.get('page', 1)
        self.per_page = kwargs.get('per_page', 20)

    # https://elasticsearch-py.readthedocs.org/en/master/api.html?highlight=search#elasticsearch.Elasticsearch.search 

    def query(self, body, **kwargs):

        per_page = kwargs.get('per_page', self.per_page)
        page = kwargs.get('page', self.page)
        
        offset = (page - 1) * per_page
        limit = per_page
        
        params = {
            'index': self.index,
            'body': body,
            'from_': offset,
            'size': limit,
        }
        
        if kwargs.get('doctype', None):
            params['doc_type'] = kwargs['doctype']

        rsp = es.search(**params)
        hits = rsp['hits']
        total = hits['total']
        
        docs = []
        
        for h in hits['hits']:
            feature = h['_source']
            docs.append(feature)
            
        pagination = self.paginate(rsp, **kwargs)

        return {'rows': docs, 'pagination': pagination}

    def paginate(self, rsp, **kwargs):

        per_page = kwargs.get('per_page', self.per_page)
        page = kwargs.get('page', self.page)

        hits = rsp['hits']
        total = hits['total']

        docs = hits['hits']
        count = len(docs)
            
        pages = float(total) / float(per_page)
        pages = math.ceil(pages)
        pages = int(pages)
            
        pagination = {
            'total': total,
            'count': count,
            'per_page': per_page,
            'page': page,
            'pages': pages
        }

        return pagination
