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
Connect to the database using psycopg2 and DictCursor:
```python
>>> import psycopg2, psycopg2.extras

>>> conn = psycopg2.connect(**db_login)
# Must use a DictCursor!
>>> curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
```

Finally, use PgPyDict:
```python
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
# Insert dave into the database
>>> dave.flush()
>>> dave
{'name':'Dave', 'id':1}

# dave behaves just like a dictionary
>>> dave['name']
Dave
>>> dave['id']
1

# Change any value
>>> dave['name'] = 'Bob'
# Send the changes to the database
>>> dave.flush()
# Commit any changes
>>> conn.commit()

# Get a row from the database, you may specify which columns must contain what
# value.
>>> bob = person.get_where(id=1)
# Or, if the table has ONLY ONE primary key, you may forgo specifying a column
# name. PyPyTable.get_where assumes you are accessing the single primary key.
>>> bob = person.get_where(1)
>>> bob
{'name':'Bob', 'id':1}

# A PgPyDict behaves like a Python dictionary and can be updated/set.  Update
# bob dict with steve dict, but don't overwrite bob's primary keys.
>>> steve = person(name='Steve')
>>> steve
{'name':'Steve', 'id':2}
>>> steve.remove_pks()
{'name':'Steve'}
>>> bob.update(steve.remove_pks())
>>> bob.flush()
# Bob is a copy of steve, except for bob's primary key
>>> bob
{'name':'Steve', 'id':1}
```
