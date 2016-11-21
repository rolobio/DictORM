# pgpydict
## Use Postgresql as if it were a Python Dictionary

[![Build Status](https://travis-ci.org/rolobio/pgpydict.svg?branch=master)](https://travis-ci.org/rolobio/pgpydict)

Many database tables are constructed similar to a Python Dictionary.  Why not
use it as such?

## Basic Usage
Install pgpydict using pip:
```bash
pip install pgpydict
```

or run python:
```bash
pip install -r requirements.txt
python setup.py install
```

Connect to the database using psycopg2:
```python
>>> import psycopg2, psycopg2.extras

>>> conn = psycopg2.connect(**db_login)
>>> curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
>>> db = DictDB(cursor)
```

Get a PgPyTable from your Dictionary DB:
```python
>>> person = db['person']
>>> person
PgPyTable(dave, [id,])
```

Insert and Update a PgPyDict object:
```python
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
```
