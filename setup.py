from setuptools import setup
from dictorm import __version__, __doc__

config = {
    'name':'dictorm',
    'version':__version__,
    'author':'rolobio',
    'author_email':'rolobio+dictorm@rolobio.com',
    'description':"Psycopg2's DictCursor is a fantastic tool, but what if you could send the dictionary back into the database?  DictORM allows you to select/insert/update rows of a database as if they were Python Dictionaries.",
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

