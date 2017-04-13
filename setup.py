from setuptools import setup
from dictorm.dictorm import __version__, __doc__ as ddoc

config = {
    'name':'dictorm',
    'version':str(__version__),
    'author':'rolobio',
    'author_email':'rolobio+dictorm@rolobio.com',
    'description':ddoc,
    'license':'Apache2',
    'keywords':'psycopg2 dictionary python dict',
    'url':'https://github.com/rolobio/DictORM',
    'packages':[
        'dictorm',
        ],
    'long_description':ddoc,
    'classifiers':[
        "Development Status :: 5 - Production/Stable",
        "Topic :: Utilities",
        ],
    'test_suite':'dictorm.test_dictorm',
    }

setup(**config)

