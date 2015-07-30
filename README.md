# py-mapzen-whosonfirst-search

## Usage

### Simple

_Note the bit about namespaces which is discussed below._

```
import whosonfirst
import mapzen.whosonfirst.utils

idx = whosonfirst.search()

source = os.path.abspath(options.source)
crawl = mapzen.whosonfirst.utils.crawl(source)

rsp = idx.index_files(crawl)
```

## Caveats

### Setup.py

There isn't one yet because Python namespacing hoohah just makes me angry and starts to feel like yak-shaving. It will happen but hasn't happened yet.

### Geometries

No GeoJSON `geometry` elements are not included in any responses by default. This is by design for performance reasons. 

### Search 

This is not meant for doing spatial queries. No. There is a separate `py-mapzen-whosonfirst-lookup` package that will makes happy with PostGIS and together they make beautiful spatial lookup. This is not that package.

## See also

* https://github.com/mapzen/theory-whosonfirst
* https://github.com/mapzen/whosonfirst-data
* https://github.com/mapzen/py-mapzen-whosonfirst-lookup
