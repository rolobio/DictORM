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


class TestPgPyTable(unittest.TestCase):


    def setUp(self):
        self.conn = psycopg2.connect(**test_db_login)
        self.curs = self.conn.cursor(cursor_factory=DictCursor)
        self.tearDown()
        self.curs.execute('''
        CREATE TABLE person (
            id SERIAL PRIMARY KEY,
            name TEXT
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


    def test_flush(self):
        Person = self.db['person']
        self.assertEqual(0, len(Person))

        bob = Person(name='Bob')
        self.assertEqual({'name':'Bob',},
                bob)
        bob.flush()
        self.assertEqual({'name':'Bob', 'id':1},
                bob)


    def test_getWhere(self):
        Person = self.db['person']
        self.assertEqual(0, len(Person))

        bob = Person(name='Bob')
        bob.flush()
        self.assertEqual(Person.getWhere(1), bob)



if __name__ == '__main__':
    unittest.main()
