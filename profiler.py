#! /usr/bin/env python
from dictorm import DictDB
import sqlite3

test_tables_sql = '''
CREATE TABLE car (
    id INTEGER PRIMARY KEY,
    entrydate TIMESTAMP DEFAULT current_timestamp,
    make TEXT,
    model TEXT
);
CREATE TABLE person (
    id INTEGER PRIMARY KEY,
    entrydate TIMESTAMP DEFAULT current_timestamp,
    name TEXT,
    car_id INTEGER REFERENCES car(id)
);'''

conn = sqlite3.connect(':memory:')
curs = conn.cursor()
# Clear out anything in the test DB, create test tables
curs.executescript(test_tables_sql)

db = DictDB(conn)

Person, Car = db['person'], db['car']
# Insert 10,000 persons and cars
[Person(name='foo',
    car_id=Car(make='bar', model='baz').flush()['id']
    ).flush() for i in range(10000)]

map(lambda i: i.delete, Person.get_where())
map(lambda i: i.delete, Car.get_where())
