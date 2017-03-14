#! /usr/bin/env python
from dictorm.test_dictorm import test_db_login
import dictorm
import sqlite3
import random
from string import ascii_letters

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

db = dictorm.DictDB(conn)

def rand_str():
    return ''.join(random.sample(ascii_letters, random.randint(5, 10)))

Person, Car = db['person'], db['car']
for idx in range(10000):
    Person(name=rand_str()).flush()

for person in Person.get_where():
    car = Car(make=rand_str(), model=rand_str()).flush()
    person['car_id'] = car['id']
    person.flush()
