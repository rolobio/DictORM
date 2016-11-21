# pgpydict
## Use Postgresql as if it were a Python Dictionary

[![Build Status](https://travis-ci.org/rolobio/pgpydict.svg?branch=master)](https://travis-ci.org/rolobio/pgpydict)

Many database tables are constructed similar to a Python Dictionary.  Why not
use it as such?

## Installation
Install pgpydict using pip:
```bash
pip install pgpydict
```

or run python:
```bash
pip install -r requirements.txt
python setup.py install
```

## Basic Usage
Connect to the database using psycopg2:
```python
>>> import psycopg2, psycopg2.extras

>>> conn = psycopg2.connect(**db_login)
# Must use a DictCursor!
>>> curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# DictDB queries the database for all tables and allows them to be gotten
# as if DictDB was a dictionary.
>>> db = DictDB(curs)

# Get a PgPyTable object for table 'person'
# person table built using: (id SERIAL PRIMARY KEY, name TEXT)
>>> person = db['person']

# PgPyDict relies on primary keys to successfully Update a row in the 'person'
# table.  The primary keys found are listed when the person object is printed.
>>> person
PgPyTable(dave, ['id',])

# You can define your own primary keys
person.pks = ['id',]

# Insert into "person" table by calling "person" object as if it were a
# dictionary.
>>> dave = person(name='Dave')
>>> dave
{'name':'Dave', 'id':1}

>>> dave['name']
Dave
>>> dave['id']
1

>>> dave['name'] = 'Bob'
# Send the changes to the database
>>> dave.flush()
# Commit any changes
>>> conn.commit()

>>> bob = person.get_where(id=1)
>>> bob
{'name':'Bob', 'id':1}

# A PgPyDict behaves exactly like a Python dictionary and can be updated/set
>>> steve = person(name='Steve')
>>> steve
{'name':'Steve', 'id':2}
# Update bob using steve's keys and values.  You get a dictionary
# without primary keys to avoid overwritting another row.
>>> steve.remove_pks()
{'name':'Steve'} # id is missing because its the primary key
>>> bob.update(steve.remove_pks())
>>> bob.flush()
>>> bob
{'name':'Steve', 'id':1}
```
