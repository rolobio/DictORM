#! /usr/bin/env python
from pgpydict import *
from psycopg2 import OperationalError
from psycopg2.extras import DictCursor
import psycopg2
import unittest

test_db_login = {
        'database':'pgpydict',
        'user':'pgpydict',
        'password':'pgpydict',
        'host':'localhost',
        'port':'5432',
        }

class Test(unittest.TestCase):


    def setUp(self):
        self.conn = psycopg2.connect(**test_db_login)
        self.curs = self.conn.cursor(cursor_factory=DictCursor)
        self.tearDown()
        self.curs.execute('''
        CREATE TABLE person (
            id SERIAL PRIMARY KEY,
            name TEXT
        );
        CREATE TABLE department (
            id SERIAL PRIMARY KEY,
            name TEXT
        );
        CREATE TABLE person_department (
            person_id INTEGER REFERENCES person(id),
            department_id INTEGER REFERENCES department(id),
            PRIMARY KEY (person_id, department_id)
        );
        ''')
        self.conn.commit()
        self.db = DictDB(self.curs)


    def tearDown(self):
        self.curs.execute('''DROP SCHEMA public CASCADE;
                CREATE SCHEMA public;
                GRANT ALL ON SCHEMA public TO postgres;
                GRANT ALL ON SCHEMA public TO public;''')
        self.conn.commit()


    def test_getWhere(self):
        Person = self.db['person']
        self.assertEqual(0, len(Person))

        bob = Person(name='Bob')
        self.assertEqual({'name':'Bob',},
                bob)
        bob.flush()
        self.assertEqual({'name':'Bob', 'id':1},
                bob)
        self.assertEqual(Person.getWhere(1), bob)

        # A second flush does not fail
        bob.flush()
        self.assertEqual({'name':'Bob', 'id':1},
                bob)
        self.assertEqual(Person.getWhere(1), bob)

        bob['name'] = 'Jon'
        bob.flush()
        self.assertEqual(bob,
                {'name':'Jon', 'id':1})
        self.assertEqual(Person.getWhere(1), bob)


    def test_getWhere_multiple_pks(self):
        Person = self.db['person']
        self.assertEqual(0, len(Person))
        bob = Person(name='Bob')
        bob.flush()

        Department = self.db['department']
        self.assertEqual(0, len(Department))
        sales = Department(name='Sales')
        sales.flush()

        PersonDepartment = self.db['person_department']
        bob_sales = PersonDepartment(person_id=bob['id'], department_id=sales['id'])
        bob_sales.flush()
        self.assertEqual(bob_sales['person_id'], bob['id'])
        self.assertEqual(bob_sales['department_id'], sales['id'])
        # Searching person_department with two key/value pairs returns the new
        # row.
        self.assertEqual(
                PersonDepartment.getWhere({'person_id':1, 'department_id':1}),
                bob_sales)


    def test_already_in_db(self):
        Person = self.db['person']
        self.assertEqual(0, len(Person))
        bob = Person(name='Bob')
        bob.flush()

        bob_copy = Person.getWhere(1)
        bob_copy.flush()
        self.assertEqual(bob, bob_copy)


    def test_dict_inits(self):
        Person = self.db['person']
        Person({'name':'Bob'})
        Person(name='Alice')
        Person([('name','Steve'),])

        PgPyDict(Person, {'name':'Bob'})
        PgPyDict(Person, name='Alice')
        PgPyDict(Person, [('name','Steve'),])


    def test_remove_pks(self):
        Person = self.db['person']
        self.assertEqual(0, len(Person))
        bob = Person(name='Bob')
        self.assertEqual(bob, {'name':'Bob'})
        bob.flush()
        self.assertEqual(bob, {'name':'Bob', 'id':1})
        self.assertEqual(bob.remove_pks(), {'name':'Bob'})

        aly = Person(name='Aly')
        self.assertEqual(aly, {'name':'Aly'})
        aly.flush()
        self.assertEqual(aly, {'name':'Aly', 'id':2})
        self.assertEqual(aly.remove_pks(), {'name':'Aly'})

        bob.update(aly.remove_pks())
        bob.flush()
        aly.flush()
        self.assertEqual(bob, {'name':'Aly', 'id':1})
        self.assertEqual(aly, {'name':'Aly', 'id':2})



if __name__ == '__main__':
    unittest.main()
