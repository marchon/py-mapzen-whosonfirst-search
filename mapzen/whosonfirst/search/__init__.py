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
import mapzen.whosonfirst.placetypes

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

        port = int(port)
        timeout = int(timeout)

        es = elasticsearch.Elasticsearch(host=host, port=port, timeout=timeout)
        self.es = es

        self.host = host
        self.port = port

        self.index = 'whosonfirst'
        self.doctype = None

    def refresh(self):

        self.es.indices.delete(index=self.index, ignore=[400, 404])
        self.es.indices.create(index=self.index)
        
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

        # alt placetype names/ID

        placetype = props['wof:placetype']
        placetype = mapzen.whosonfirst.placetypes.placetype(placetype)

        placetype_id = placetype.id()
        placetype_names = []

        for n in placetype.names():
            placetype_names.append(unicode(n))

        props['wof:placetype_id'] = placetype_id
        props['wof:placetype_names'] = placetype_names

        # because ES suffers from E_EXCESSIVE_CLEVERNESS

        props = self.enstringify(props)
        return props

    def enstringify(self, data, **kwargs):
        
        ima_int = (
            'continent_id',
            'country_id',
            'county_id',
            'gn:elevation',
            'gn:population',
            'gn:id',
            'gp:id',
            'locality_id',
            'neighbourhood_id',
            'region_id',
            'wof:id',
            'wof:belongsto',
            'wof:breaches',
            'wof:lastmodified',
            'wof:megacity',
            'wof:placetype_id',
            'wof:population',
            'wof:scale',
            'wof:superseded_by',
            'wof:supersedes',
            'zs:pop10',
        )

        ima_float = (
            'geom:area',
            'geom:latitude',
            'geom:longitude',
            'lbl:latitude',
            'lbl:longitude',
            'mps:latitude',
            'mps:longitude',
        )

        isa = type(data)

        if isa == types.DictType:

            for k, v in data.items():
                k = unicode(k)
                v = self.enstringify(v, key=k)
                data[k] = v

            return data

        elif isa == types.ListType:
            
            str_data = []

            for thing in data:
                str_data.append(self.enstringify(thing, **kwargs))

            return str_data

        elif isa == types.NoneType:
            return unicode("")

        else:

            k = kwargs.get('key', None)
            logging.debug("processing %s: %s" % (k,data))

            if k and k in ima_int:

                if data == '':
                    return 0

                return int(data)

            elif k and k in ima_float:

                if data == '':
                    return 0.0

                return float(data)

            else:
                return unicode(data)

    def load_file(self, f):

        try:
            fh = open(f, 'r')
            return geojson.load(fh)
        except Exception, e:
            logging.error("failed to open %s, because %s" % (f, e))
            raise Exception, e

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

    def escape(self, str):

        # If you need to use any of the characters which function as operators in
        # your query itself (and not as operators), then you should escape them
        # with a leading backslash. For instance, to search for (1+1)=2, you would
        # need to write your query as \(1\+1\)\=2. 
        # 
        # The reserved characters are: + - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /
        # 
        # Failing to escape these special characters correctly could lead to a
        # syntax error which prevents your query from running.
        # 
        # A space may also be a reserved character. For instance, if you have a
        # synonym list which converts "wi fi" to "wifi", a query_string search for
        # "wi fi" would fail. The query string parser would interpret your query
        # as a search for "wi OR fi", while the token stored in your index is
        # actually "wifi". Escaping the space will protect it from being touched
        # by the query string parser: "wi\ fi"
        # 
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html

        # note the absence of "&" and "|" which are handled separately

        to_escape = [
            "+", "-", "=", ">", "<", "!", "(", ")", "{", "}", "[", "]", "^", '"', "~", "*", "?", ":", "\\", "/"
        ]

        escaped = []

        unistr = str.decode("utf-8")
        length = len(unistr)

        i = 0

        while i < length:

            char = unistr[i]
            
            if char in to_escape:
                char = "\%s" % char

            elif char in ("&", "|"):

                if (i + 1) < length and unistr[ i + 1 ] == char:
                    char = "\%s" % char
            else:
                pass

            escaped.append(char)
            i += 1

        return "".join(escaped)

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

if __name__ == '__main__':

            q = query();
            print q.escape("aaron+bob\\&&")
