# py-mapzen-whosonfirst-search

## IMPORTANT

This library is provided as-is, right now. It lacks proper
documentation which will probably make it hard for you to use unless
you are willing to poke and around and investigate things on your
own.


## Prerequisites

The Shapely 1.5.x requires
   GEOS >=3.3 (Shapely 1.2.x requires only GEOS 3.1 but YMMV)

Before You Install Shapely ** Make Sure You Have Installed LIBGEOS ** 

### Windows
   Windows users should download an executable installer from http://www.lfd.uci.edu/~gohlke/pythonlibs/#shapely or PyPI (if available).

### OSX
   brew install geos  

### UBUNTU / DEBIAN 

   apt-get install libgeos-dev 

make sure that libgeos is on the system library path and then install Shapely from the Python package index.
$ pip install shapely



## Usage

### Simple





#### Indexing

```
import mapzen.whosonfirst.search
import mapzen.whosonfirst.utils

idx = whosonfirst.mapzen.search.index()

source = os.path.abspath(options.source)
crawl = mapzen.whosonfirst.utils.crawl(source)

for path in crawl:
    try:
        idx.index_file(path)
    except Exception, e:
        logging.error("failed to index %s, because %s" % (path, e))

# Or this:
# rsp = idx.index_files(crawl)
```

## Command line tools

### wof-es-index

```
$> cd /usr/local/mapzen/whosonfirst-venue/
$> git pull origin master
$> /usr/local/bin/wof-es-index -s /usr/local/mapzen/whosonfirst-venue/data/ -b
```

## Caveats

### Geometries

GeoJSON `geometry` elements are not included in any responses by default. This is by design for performance reasons. 

### Search 

This is not meant for doing spatial queries. No. There is a separate `py-mapzen-whosonfirst-spatial` package that will makes happy with PostGIS and together they make beautiful spatial lookup. This is not that package.

## See also

* https://github.com/mapzen/theory-whosonfirst
* https://github.com/mapzen/whosonfirst-data
* https://github.com/mapzen/py-mapzen-whosonfirst-spatial
