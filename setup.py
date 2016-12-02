from setuptools import setup
from pgpydict import __version__, __doc__

config = {
    'name':'pgpydict',
    'version':__version__,
    'author':'rolobio',
    'author_email':'rolobio+pgpydict@rolobio.com',
    'description':'Access a Psycopg2 database as if it were a Python Dictionary',
    'license':'Apache2',
    'keywords':'psycopg2 dictionary',
    'url':'https://github.com/rolobio/pgpydict',
    'packages':[
        'pgpydict',
        ],
    'long_description':__doc__,
    'install_requires': [
        'psycopg2',
        ],
    'classifiers':[
        "Development Status :: 4 - Beta",
        "Topic :: Utilities",
        ],
    'test_suite':'pgpydict.test_pgpydict',
    }

setup(**config)

