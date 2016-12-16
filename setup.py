from setuptools import setup
from dictorm import __version__, __doc__

config = {
    'name':'dictorm',
    'version':__version__,
    'author':'rolobio',
    'author_email':'rolobio+dictorm@rolobio.com',
    'description':'Access a Psycopg2 database as if it were a Python Dictionary',
    'license':'Apache2',
    'keywords':'psycopg2 dictionary python dict',
    'url':'https://github.com/rolobio/DictORM',
    'packages':[
        'dictorm',
        ],
    'long_description':__doc__,
    'install_requires': [
        'psycopg2',
        ],
    'classifiers':[
        "Development Status :: 4 - Beta",
        "Topic :: Utilities",
        ],
    'test_suite':'dictorm.test_dictorm',
    }

setup(**config)

