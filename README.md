# DictORM
## Use Postgres/Sqlite as if it were a Python Dictionary

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/450c0aedf92645e89bd08ec2612dc653)](https://www.codacy.com/app/rolobio/DictORM?utm_source=github.com&utm_medium=referral&utm_content=rolobio/DictORM&utm_campaign=badger)
[![Build Status](https://travis-ci.org/rolobio/DictORM.svg?branch=master)](https://travis-ci.org/rolobio/DictORM)
[![Coverage Status](https://coveralls.io/repos/github/rolobio/DictORM/badge.svg?branch=master)](https://coveralls.io/github/rolobio/DictORM?branch=master)

What if you could insert a Python dictionary into the database?  DictORM allows
you to select/insert/update rows of a database as if they were Python
Dictionaries.

## Installation
Install dictorm using pip:
```bash
pip install dictorm
```

## Quick & Simple Example!
```python
# Create a dictionary that contains all tables in the database
>>> from dictorm import DictDB
>>> db = DictDB(db_conn)
# Get the Table object that was automatically found by DictDB
>>> Person = db['person']

# Define Will's initial column values
>>> will = Person(name='Will')
>>> will
{'name':'Will',}

# Insert Will
>>> will.flush()
>>> will
{'name':'Will', 'id':1}

# Change Will however you want
>>> will['name'] = 'Steve'
>>> will
{'name':'Steve', 'id':1}
# Send the changes to the database, all columns will be overwritten to what this
# "dictionary" now contains.
>>> will.flush()

# DictORM will NEVER commit or rollback changes, that is up to you.
# Make sure to commit your changes:
db_conn.commit()
```

## References will be represented as a sub-dictionary
```python
# Define a relationship to another table, access that one-to-one relationship
# as if it were a sub-dictionary.
>>> Car = db['car']
>>> Person['car'] = Person['car_id'] == Car['id']
# 'car'            : the key of the sub-dictionary you are defining
# Person['car_id'] : the column of the "person" table that references car.id
# Car['id']        : the foreign key of the "car" table, referenced by person.car_id

# When defining a reference, it is important to order the columns correctly, the
# foreign-key/foreign-table should be on the right:
# Person['car'] = Person['car_id'] == Car['id']            # Correct
# Person['car'] = Car['id'] == Person['car_id']            # Incorrect
# Person['manager'] = Person['manager_id'] == Person['id'] # Correct
# Person['manager'] = Person['id'] == Person['manager_id'] # Incorrect

>>> wills_car = Car(name='Dodge Stratus', plate='123ABC')
>>> wills_car.flush()
>>> wills_car
{'id':1, 'name':'Dodge Stratus', 'plate':'123ABC'}

>>> will['car_id'] = wills_car['id']
# Update the database row by updating the "will" object with his new car. Flush.
>>> will.flush()
>>> will
{'name':'Will', 'id':1, 'car_id':1, 'car':{'id':1, 'name':'Dodge Stratus', 'plate':'123ABC'}}
>>> will['car'] == wills_car
True

# I did not show 'car_id' in the first Will examples, this was to avoid
# confusion.  You must define 'car_id' in the database before it can be
# accessed by DictORM.
```

## Detailed Basic Usage
Create your tables with at least one primary key:
```sql
CREATE TABLE person (
    id SERIAL PRIMARY KEY,
    name TEXT,
    car_id INTEGER REFERENCES car(id),
    manager_id INTEGER REFERENCES person(id)
);
CREATE TABLE car (
    id SERIAL PRIMARY KEY,
    license TEXT
);
```

Connect to the database using psycopg2
```python
>>> import psycopg2
>>> conn = psycopg2.connect(**db_login)
```

or Sqlite3
```python
>>> import sqlite3
>>> conn = sqlite3.connect(':memory:')
```

Finally, use DictORM:
```python
# DictDB queries the database for all tables and allows them to be gotten
# as if DictDB was a dictionary.
>>> db = DictDB(conn)

# Get a Table object for table 'person'
# person table built using: (id SERIAL PRIMARY KEY, name TEXT)
>>> Person = db['person']

# DictORM relies on primary keys to successfully Update a row in the 'person'
# table.  The primary keys found are listed when the Person object is printed.
>>> Person
Table(dave, ['id',])

# You can define your own primary keys
Person.pks = ['id',]

# Insert into "person" table by calling "Person" object as if it were a
# dictionary.
>>> dave = Person(name='Dave').flush()
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
# Commit any changes is up to you.
>>> conn.commit()
```

### Get a row from the database as a Dict
```python
# Get a row from the database, you may specify which columns must contain what
# value.
>>> bob = Person.get_one(id=1)
# Or, if the table has primary key(s), you may forgo specifying a column
# name. PyPyTable.get_one will pair the arguments you provide with the
# primary keys in their respective orders:
>>> bob = Person.get_one(1)
>>> bob
{'name':'Bob', 'id':1}
# Get all rows in a table.
>>> list(Person.get_where())
[{'name':'Bob', 'id':1},]
# get_where returns a ResultsGenerator, which behaves just like a python
# generator.  It will not retreive a result from the database until you request
# it.
>>> Person.get_where()
ResultsGenerator()
>>> for person in Person.get_where():
>>>     person
{'name':'Bob', 'id':1}
```

### Update a Dict without overwriting Primary Keys
```python
# A Dict behaves like a Python dictionary and can be updated/set.  Update
# bob dict with steve dict, but don't overwrite bob's primary keys.
>>> steve = Person(name='Steve').flush()
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

### Set a one-to-one reference to another table
```python
# person              | car
# --------------------+-------
# car_id -----------> | id
>>> Car = db['car']
>>> Person['car'] = Person['car_id'] == Car['id']
# Give Steve a car
>>> steve = Person.get_one(1)
>>> steves_car = Car().flush()
>>> steve['car_id'] = steves_car['id']
>>> steve.flush()
>>> steve['car'] == steves_car
True
```

### Reference a person's manager, and a manager's subordinates
```python
# person             | person
# -------------------+-----------
# id --------------> | manager_id
>>> Person['manager'] = Person['id'] == Person['manager_id']
>>> steve = Person.get_one(1)
>>> bob = Person(name='Bob', manager_id=steve['id']).flush()
>>> aly = Person(name='Aly', manager_id=steve['id']).flush()
>>> bob['manager'] == steve
True
>>> aly['manager'] == steve
True

# Define that "subordinates" contains many rows by using ">".  Greater-Than
# is used over "in" because __contains__ overwrites what is returned
# with a True/False.  So ">" is used.
>>> Person['subordinates'] = Person['id'].many(Person['manager_id'])
>>> list(steve['subordinates'])
[bob, aly]
```

### Add in more tables
```sql
CREATE TABLE department (
    id SERIAL PRIMARY KEY,
    name TEXT
);
CREATE TABLE person_department (
    person_id INTEGER REFERENCES person(id),
    department_id INTEGER REFERENCES department(id),
    PRIMARY KEY (person_id, department)
);
```

### Set a many-to-many reference to another table using an intermediary table
```python
# person              | person_department            | department
# --------------------+------------------------------+-------------------
# id <-------+-+----- | person_id   department_id -> | id
#             \ \---- | person_id   department_id -> | id
#              \----- | person_id   department_id -> | id
>>> Department = db['department']
>>> PD = db['person_department']
>>> PD['department'] = PD['department_id'] == Department['id']
>>> PD['person'] = PD['person_id'] == Person['id']

# Reference many rows using ">".  I would rather use "in", but "__contains__"
# overwrites any values returned and instead returns a True/False.  So, we use
# ">" to specify that many rows can be returned.
>>> Person['person_departments'] = Person['id'].many(PD['person_id'])

# Create HR and Sales departments
>>> hr = Department(name='HR').flush()
>>> hr
{'name':'HR', 'id':1}
>>> sales = Department(name='Sales').flush()

# Add PD rows for Steve for both departments
>>> PD(person_id=steve['id'], department_id=hr['id']).flush()
>>> PD(person_id=steve['id'], department_id=sales['id']).flush()

>>> steve['person_departments']
[{'department': hr, 'department_id': 1, 'person_id': 1},
 {'department': sales, 'department_id': 2, 'person_id': 1}]

# Iterate through Steve's departments
>>> for pd in steve['person_departments']:
>>>    pd['department']
{'name':'HR', 'id':1}
{'name':'Sales', 'id':2}

# Get all persons who are in sales:
>>> PD(person_id=aly['id'], department_id=sales['id']).flush()
>>> PD(person_id=bob['id'], department_id=sales['id']).flush()
>>> for pd in PD.get_where(department_id=sales['id']):
>>>     pd['person']
steve
aly
bob
```

### Substratum
```python
# Having to remember to iterate through steve['person_departments'] and then
# access ['department'] is a little cumbersome, why not skip over the join-table
# (person_departments) and go straight to the referenced department?
>>> Person['departments'] = Person['person_departments'].substratum('department')
# Person['person_departments'] must be created first (it was created in the
# previous example), then you can substratum a reference on it.

>>> steve['departments']
[{'name':'HR', 'id':1},
 {'name':'Sales', 'id':2},]

# Much easier and intuitive!
>>> for dept in steve['departments']:
>>>    dept
{'name':'HR', 'id':1}
{'name':'Sales', 'id':2}
```

### Reuse a ResultsGenerator
```python
# get_where and get_one return a ResultsGenerator, which does nothing until you
# attempt to get a result from it.  This means we can reuse a ResultsGenerator
# to refine the results.
>>> minions = steve['subordinates']
>>> minions
[bob, aly]

# Limit the results to only one row
>>> list(minions.limit(1))
[bob,]

# Reverse the order, limit to one row
>>> list(minions.order_by('id ASC').limit(1))
[aly,]
```

### Advanced query'ing
```python
# DictORM supports many simple expressions.  It is by no means exhaustive,
# but it supports the basic features.

# Inserting Frank for these examples
>>> frank = Person(name='Frank', manager_id=bob['id']).flush()
>>> frank
{'name':'Frank', 'id':4, 'manager_id':1, 'manager':bob}

# Search using pythonic expressions
>>> list(Person.get_where(Person['name'] == 'Steve'))
[steve,]
>>> Person.get_where(Person['id'] > 1)
[steve, aly, frank]

# Custom results ordering (reverse the previous results)
>>> Person.get_where(Person['id'] > 1).order_by('id DESC')
[frank, aly, steve]

# Limit the results
>>> Person.get_where(Person['id'] > 1).order_by('id DESC').limit(1)
[frank,]

# Offset the results
>>> Person.get_where(Person['id'] > 1).order_by('id DESC').limit(1).offset(1)
[aly,]

# Refine a subordinate search
>>> bob['subordinates'].refine(Person['name']=='Aly')
[aly,]

# Subordinate that has a car
>>> bob['subordinates'].refine(Person['car_id']>0)
[steve,]
>>> bob['subordinates'].refine(Person['car_id'].IsNot(None))
[steve,]
```

#### Pythonic Comparisons create SQL Comparisons
```python
>>> Person['foo'] == 'bar'
"foo = 'bar'"
>>> Person['foo'] > 'bar'
"foo > 'bar'"
>>> Person['foo'] >= 'bar'
"foo >= 'bar'"
>>> Person['foo'] < 'bar'
"foo < 'bar'"
>>> Person['foo'] <= 'bar'
"foo <= 'bar'"
>>> Person['foo'] != 'bar'
"foo <= 'bar'"
>>> Person['foo'].Is('bar')
"foo IS 'bar'"
>>> Person['foo'].IsNot('bar')
"foo IS NOT 'bar'"
>>> Person['foo'].IsDistinct('bar')
"foo IS DISTINCT FROM 'bar'"
>>> Person['foo'].IsNotDistinct('bar')
"foo IS NOT DISTINCT FROM 'bar'"
>>> Person['foo'].IsNull()
"foo IS NULL"
>>> Person['foo'].IsNotNull()
"foo IS NOT NULL"
>>> Person['foo'].In(('bar', 'baz')) # Not supported for Sqlite3
"foo IN ('bar', 'baz')"
>>> Person['foo'].Like('bar')
"foo LIKE 'bar'"
>>> Person['foo'].Ilike('bar') # Not supported for Sqlite3
"foo ILIKE 'bar'"
```

### Delete
```python
# Delete a single row
>>> frank.delete()
```
