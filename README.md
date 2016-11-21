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

## Quick feature example
```python
# Create a dictionary that contains all tables in the database
>>> db = DictDB(psycopg2_DictCursor)
# Get the PgPyTable object that was automatically found by DictDB
>>> Person = db['person']
# Define Will's initial column values
>>> will = Person(name='Will')
# Insert Will
>>> will.flush()
>>> will
{'name':'Will', 'id':1}
# Change Will however you want
>>> will['name'] = 'Steve'
>>> will
{'name':'Steve', 'id':1}
# Send the changes to the database
>>> will.flush()
```

## Another quick feature example (the cool stuff)
```python
# Define a relationship to another table, access that one-to-one relationship
# as if it were a sub-dictionary.
>>> Car = db['car']
>>> Person.set_reference('car_id', 'car', Car, 'id')
# 'car_id' : the column that Person contains that references the 'car' table
# 'car'    : the key of the sub-dictionary you are defining
# Car      : the PgPyTable object that you are referencing
# 'id'     : the primary key that 'car_id' references

>>> wills_car = Car(name='Dodge Stratus', plate='123ABC')
>>> wills_car.flush()
>>> wills_car
{'id':1, 'name':'Dodge Stratus', 'plate':'123ABC'}

>>> will['car_id'] = wills_car['id']
# Update the database row, update the will object with his new car
>>> will.flush()
>>> will
{'name':'Will', 'id':1, 'car_id':1, 'car':{'id':1, 'name':'Dodge Stratus', 'plate':'123ABC'}}
>>> will['car'] == wills_car
True

# I did not show 'car_id' in the first Will examples, this was to avoid
# confusion.  You must define 'car_id' in the database before it can be
# accessed by PgPyDict.
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
>>> Person = db['person']

# PgPyDict relies on primary keys to successfully Update a row in the 'person'
# table.  The primary keys found are listed when the Person object is printed.
>>> Person
PgPyTable(dave, ['id',])

# You can define your own primary keys
Person.pks = ['id',]

# Insert into "person" table by calling "Person" object as if it were a
# dictionary.
>>> dave = Person(name='Dave')
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
```

Get a row from the database as a PgPyDict
```python
# Get a row from the database, you may specify which columns must contain what
# value.
>>> bob = Person.get_where(id=1)
# Or, if the table has ONLY ONE primary key, you may forgo specifying a column
# name. PyPyTable.get_where assumes you are accessing the single primary key.
>>> bob = Person.get_where(1)
>>> bob
{'name':'Bob', 'id':1}

# A PgPyDict behaves like a Python dictionary and can be updated/set.  Update
# bob dict with steve dict, but don't overwrite bob's primary keys.
>>> steve = Person(name='Steve')
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

Set a one-to-one reference to another table
```python
# person              | car
# --------------------+-------
# car_id -----------> | id
>>> Car = db['car']
>>> Person.set_reference('car_id', 'car', Car, 'id')
# Give Steve a car
>>> steve = Person.get_where(1)
>>> steve['car_id'] = car['id']
>>> steve.flush()
>>> steve['car'] == car
True
```

Set a one-to-many reference to another table using an intermediary table
```python
# person              | person_department            | department
# --------------------+------------------------------+-------------------
# id <-------+-+----- | person_id   department_id -> | id
#             \ \---- | person_id   department_id -> | id
#              \----- | person_id   department_id -> | id
>>> Department = db['department']
>>> PD = db['person_department']
>>> PD.set_reference('department_id', 'department', Department, 'id')

# Define one-to-many reference for person
>>> Person.set_reference('id', 'person_departments', PD, 'person_id', is_list=True)

# Create HR and Sales departments
>>> hr = Department(name='HR')
>>> hr.flush()
>>> sales = Department(name='Sales')
>>> sales.flush()

# Add PD rows for Steve for both departments
>>> PD(person_id=steve['id'], department_id=hr['id'])
>>> PD(person_id=steve['id'], department_id=sales['id'])

>>> steve['person_departments']
[{'department': {'name':'HR', 'id':1}, 'department_id': 1, 'person_id': 1},
 {'department': {'name':'Sales', 'id':2}, 'department_id': 2, 'person_id': 1}]
```
