#! /usr/bin/env python3
from setuptools import setup
from dictorm.dictorm import __version__, __doc__ as ddoc

config = {
    'name': 'dictorm',
    'version': str(__version__),
    'author': 'rolobio',
    'author_email': 'rolobio+dictorm@rolobio.com',
    'description': ddoc,
    'license': 'Apache2',
    'keywords': 'psycopg2 dictionary python dict',
    'url': 'https://github.com/rolobio/DictORM',
    'packages': [
        'dictorm',
    ],
    'long_description': ddoc,
    'classifiers': [
        "Development Status :: 5 - Production/Stable",
        "Topic :: Utilities",
    ],
    'setup_requires': ['green>=2.12.0'],
    'tests_require': [
        'coverage',
        'coveralls',
        'green>=2.12.0',
        'psycopg2-binary',
    ],
    'extras_require': {
        'Postgresql': ['psycopg2-binary'],
        'testing': ['psycopg2-binary', 'green>=2.12.0', 'coveralls', 'coverage'],
    }
}

setup(**config)
