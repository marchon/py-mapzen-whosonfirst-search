#!/usr/bin/env python

from setuptools import setup, find_packages

packages = find_packages()
desc = open("README.md").read(),

setup(
    name='mapzen.whosonfirst.search',
    namespace_packages=['mapzen', 'mapzen.whosonfirst', 'mapzen.whosonfirst.search'],
    version='0.08',
    description='Simple Python wrapper for Who\'s On First search functionality',
    author='Mapzen',
    url='https://github.com/mapzen/py-mapzen-whosonfirst-search',
    install_requires=[
        'geojson',
        'elasticsearch',
        ],
    dependency_links=[
        ],
    packages=packages,
    scripts=[
        'scripts/wof-es-index',
        'scripts/wof-es-index-files',
        'scripts/wof-es-prepare',
        'scripts/wof-es-rawquery',
        ],
    download_url='https://github.com/mapzen/py-mapzen-whosonfirst-search/releases/tag/v0.08',
    license='BSD')
