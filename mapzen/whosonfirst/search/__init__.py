# https://pythonhosted.org/setuptools/setuptools.html#namespace-packages
__import__('pkg_resources').declare_namespace(__name__)

import types
import os.path
import csv
import json
import geojson
import logging
import math

import urllib
import requests

import mapzen.whosonfirst.utils

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

        self.host = host
        self.port = port

        self.index = 'whosonfirst'
        self.doctype = None

class index(base):
    
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

        # ggggggrgrgrgrgrhhnhnnnhnhnhnhnhhzzzzpphphtttt - we shouldn't
        # have to do this but even with the enstringification below
        # ES tries to be too clever by half so in the interests of just
        # getting stuff done we're going to be ruthless about things...
        # (21050806/thisisaaronland)

        omgwtf = (
            u'ne:fips_10_',
            u'ne:gdp_md_est',
            u'ne:geou_dif',
            u'ne:pop_est',
            u'ne:su_dif',
            u'ne:adm0_dif',
            u'ne:level',
        )

        for bbq in omgwtf:
            if props.has_key(bbq):
                logging.warning("remove tag '%s' because ES suffers from E_EXCESSIVE_CLEVERNESS" % bbq)
                del(props[bbq])

        # To do: stringify all the values so that things can
        # just go in to ES without the automagic schema mapping
        # thing choosing the wrong field type and then complaining
        # about it later (20150730/thisisaaronland)

        props = self.enstringify(props)
        return props

    def enstringify(self, data):
        
        isa = type(data)

        if isa == types.DictType:

            for k, v in data.items():
                k = unicode(k)
                v = self.enstringify(v)
                data[k] = v

            return data

        elif isa == types.ListType:
            
            str_data = []

            for thing in data:
                str_data.append(self.enstringify(thing))

            return str_data

        elif isa == types.NoneType:
            return unicode("")

        else:
            return unicode(data)

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

class query(base):

    def __init__(self, **kwargs):

        base.__init__(self, **kwargs)

        self.page = kwargs.get('page', 1)
        self.per_page = kwargs.get('per_page', 20)

    # because who knows what elasticsearch-py is doing half the time...
    # (20150805/thisisaaronland)

    def search_raw(self, **args):

        path = args.get('path', '_search')
        body = args.get('body', {})
        query = args.get('query', {})

        url = "http://%s:%s/%s/%s" % (self.host, self.port, self.index, path)

        if len(query.keys()):
            q = urllib.urlencode(query)
            url = url + "?" + q

        body = json.dumps(body)

        rsp = requests.post(url, data=body)
        return json.loads(rsp.content)

    # https://elasticsearch-py.readthedocs.org/en/master/api.html?highlight=search#elasticsearch.Elasticsearch.search 

    def search(self, body, **kwargs):

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

        rsp = self.es.search(**params)
        hits = rsp['hits']
        total = hits['total']
        
        docs = []
        
        for h in hits['hits']:
            feature = self.enfeaturify(h)
            docs.append(feature)
            
        pagination = self.paginate(rsp, **kwargs)

        return {'rows': docs, 'pagination': pagination}

    def enfeaturify(self, row):

        properties = row['_source']
        id = properties['wof:id']

        geom = {}
        bbox = []

        lat = None
        lon = None

        if not properties.get('wof:path', False):

            path = mapzen.whosonfirst.utils.id2relpath(id)
            properties['wof:path'] = path

        if properties.get('geom:bbox', False):
            bbox = properties['geom:bbox']
            bbox = bbox.split(",")

        if properties.get('geom:latitude', False) and properties.get('geom:longitude', False):
            lat = properties['geom:latitude']
            lat = properties['geom:longitude']

        elif len(bbox) == 4:
            pass	# derive centroid here...

        else:
            pass

        if properties.get('wof:placetype', None) == 'venue' and lat and lon:
                geom = {'type': 'Point', 'coordinates': [ lon, lat ] }

        return {
            'type': 'Feature',
            'id': id,
            'bbox': bbox,
            'geometry': geom,
            'properties': properties
        }

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
