#! /usr/bin/env python
from pprint import pprint, pformat
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
        self.curs.execute('CREATE TABLE table1 (id SERIAL PRIMARY KEY, foo TEXT)')
        self.conn.commit()


    def tearDown(self):
        self.curs.execute('''DROP SCHEMA public CASCADE;
                CREATE SCHEMA public;
                GRANT ALL ON SCHEMA public TO postgres;
                GRANT ALL ON SCHEMA public TO public;''')
        self.conn.commit()


    def test_init(self):
        Table1 = PgPyTable('table1', self.curs, ('id',))
        Table1({'foo':'bar',})
        row = Table1.getByPrimary(1)
        self.assertEqual(row, {'id':1, 'foo':'bar'})



class TestPgPyDict(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect(**test_db_login)
        self.curs = self.conn.cursor(cursor_factory=DictCursor)
        self.tearDown()
        self.curs.execute('CREATE TABLE table1 (id SERIAL PRIMARY KEY, foo TEXT)')
        self.curs.execute('CREATE TABLE table2 (id SERIAL, group_id SERIAL, person TEXT, PRIMARY KEY(id, group_id))')
        self.conn.commit()


    def tearDown(self):
        self.curs.execute('''DROP SCHEMA public CASCADE;
                CREATE SCHEMA public;
                GRANT ALL ON SCHEMA public TO postgres;
                GRANT ALL ON SCHEMA public TO public;''')
        self.conn.commit()


    def test___setitem__(self):
        Table1 = PgPyTable('table1', self.curs, ('id',))
        Table1({'foo':'bar',})
        row = Table1.getByPrimary(1)
        self.assertEqual(row, {'id':1, 'foo':'bar'})
        row['foo'] = 'baz'
        row = Table1.getByPrimary(1)
        self.assertEqual(row, {'id':1, 'foo':'baz'})


    def test___delitem__(self):
        """
        A PgPyDict should not delete an item, instead it should set it to None
        so that value will be reflected in the database.
        """
        Table1 = PgPyTable('table1', self.curs, ('id',))
        Table1({'foo':'bar',})
        row = Table1.getByPrimary(1)
        del row['foo']
        self.assertEqual(row['foo'], None)


    def test_update(self):
        """
        Update should change all values except primary keys.
        """
        Table1 = PgPyTable('table1', self.curs, ('id',))
        Table1({'foo':'bar',})
        row = Table1.getByPrimary(1)
        new_row = {'id':2, 'foo':'baz'}
        row.update(new_row)
        self.assertEqual(row['foo'], 'baz')
        # Primary key cannot be overwritten
        self.assertEqual(row['id'], 1)


    def test_empty(self):
        """
        Can insert an empty dict.
        """
        Table1 = PgPyTable('table1', self.curs, ('id',))
        row = Table1()
        self.assertEqual(row['id'], 1)


    def test_dict(self):
        """
        Test that the PgPyDict functions as a normal dict.
        """
        Table1 = PgPyTable('table1', self.curs, ('id',))
        row = Table1({'foo':'bar',})
        self.assertEqual(len(row), 2)
        self.assertEqual(sorted(iter(row)), sorted(['id', 'foo']))

        # Dict can be created by a list of keys and list of values
        row = Table1(zip(('foo',), ('baz',)))
        self.assertEqual(len(row), 2)
        self.assertEqual(sorted(iter(row)), sorted(['id', 'foo']))


    def test_multiple_primarys(self):
        Table2 = PgPyTable('table2', self.curs, ('id', 'group_id'))
        row = Table2({'id':4, 'group_id':2, 'person':'Dave'})
        row['person'] = 'Austin'
        row = Table2.getByPrimary({'id':4, 'group_id':2})
        self.assertEqual(row['person'], 'Austin')
        row.update({'group_id':5, 'person':'Thomas'})
        self.assertEqual(row['group_id'], 2)
        row = Table2.getByPrimary({'id':4, 'group_id':2})
        self.assertEqual(row['person'], 'Thomas')


    def test_multiple_cursors(self):
        """
        Multiple cursors should function as expected until there is a
        transaction error.
        """
        curs1 = self.curs
        curs2 = self.conn.cursor(cursor_factory=DictCursor)
        Table1a = PgPyTable('table1', curs1, ('id',))
        Table1b = PgPyTable('table1', curs2, ('id',))
        row1a = Table1a({'foo':'bar',})
        row2a = Table1b({'foo':'baz',})
        self.conn.commit()
        row1b = Table1a.getByPrimary(1)
        row2b = Table1a.getByPrimary(2)
        self.assertEqual(row1a, row1b)
        self.assertEqual(row2a, row2b)



class TestSubPgPyDict(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect(**test_db_login)
        self.curs = self.conn.cursor(cursor_factory=DictCursor)
        self.tearDown()
        self.curs.execute('CREATE TABLE table2 (id SERIAL PRIMARY KEY, person TEXT)')
        self.curs.execute('CREATE TABLE table1 (id SERIAL PRIMARY KEY, foo TEXT, table2_id INTEGER REFERENCES table2(id) )')
        self.conn.commit()


    def tearDown(self):
        self.curs.execute('''DROP SCHEMA public CASCADE;
                CREATE SCHEMA public;
                GRANT ALL ON SCHEMA public TO postgres;
                GRANT ALL ON SCHEMA public TO public;''')
        self.conn.commit()


    def test_sub_dict_set_obj(self):
        """
        A sub-pgpydict can be specified when a reference column is defined.
        """
        Table1 = PgPyTable('table1', self.curs, ('id',))
        Table2 = PgPyTable('table2', self.curs, ('id',))
        Table1.addReference(Table2, 'id', 'table2_id', 'table2')
        row3 = Table2({'person':'Bob',})
        row2 = Table2({'person':'Dave',})
        row1 = Table1({'foo':'bar',})
        # Rows have not yet been associated
        self.assertEqual(row1['table2_id'], None)
        self.assertEqual(row1['table2'], None)

        row1['table2'] = row2
        self.assertEqual(row1['table2_id'], row2['id'])
        self.assertEqual(row1['table2'], row2)
        self.conn.commit()
        self.assertEqual(row1['table2_id'], row2['id'])
        self.assertEqual(row1['table2'], row2)

        row1['table2'] = row3
        self.assertEqual(row1['table2_id'], row3['id'])
        self.assertEqual(row1['table2'], row3)
        self.conn.commit()
        self.assertEqual(row1['table2_id'], row3['id'])
        self.assertEqual(row1['table2'], row3)

        row1['table2'] = None
        self.assertEqual(row1['table2_id'], None)
        self.assertEqual(row1['table2'], None)
        self.conn.commit()
        self.assertEqual(row1['table2_id'], None)
        self.assertEqual(row1['table2'], None)


    def test_sub_dict_set_id(self):
        """
        A sub-pgpydict can be specified when a reference column is defined.
        """
        Table1 = PgPyTable('table1', self.curs, ('id',))
        Table2 = PgPyTable('table2', self.curs, ('id',))
        Table1.addReference(Table2, 'id', 'table2_id', 'table2')
        row3 = Table2({'person':'Bob',})
        row2 = Table2({'person':'Dave',})
        row1 = Table1({'foo':'bar',})
        # Rows have not yet been associated
        self.assertEqual(row1['table2_id'], None)
        self.assertEqual(row1['table2'], None)

        row1['table2_id'] = row2['id']
        self.assertEqual(row1['table2_id'], row2['id'])
        self.assertEqual(row1['table2'], row2)
        self.conn.commit()
        self.assertEqual(row1['table2_id'], row2['id'])
        self.assertEqual(row1['table2'], row2)

        row1['table2_id'] = row3['id']
        self.assertEqual(row1['table2_id'], row3['id'])
        self.assertEqual(row1['table2'], row3)
        self.conn.commit()
        self.assertEqual(row1['table2_id'], row3['id'])
        self.assertEqual(row1['table2'], row3)

        row1['table2_id'] = None
        self.assertEqual(row1['table2_id'], None)
        self.assertEqual(row1['table2'], None)
        self.conn.commit()
        self.assertEqual(row1['table2_id'], None)
        self.assertEqual(row1['table2'], None)


    def test_sub_dict_set_many(self):
        """
        A sub-pgpydict can be specified when a reference column is defined.
        """
        Table1 = PgPyTable('table1', self.curs, ('id',))
        Table2 = PgPyTable('table2', self.curs, ('id',))
        Table1.addReference(Table2, 'id', 'table2_id', 'table2')
        row2a = Table2({'person':'Dave',})
        row2b = Table2({'person':'Bob',})
        row1a = Table1({'foo':'bar',})
        row1b = Table1({'foo':'baz',})
        # Rows have not yet been associated
        self.assertEqual(row1a['table2_id'], None)
        self.assertEqual(row1a['table2'], None)
        self.assertEqual(row1b['table2_id'], None)
        self.assertEqual(row1b['table2'], None)

        # Associate a's
        row1a['table2_id'] = row2a['id']
        self.assertEqual(row1a['table2_id'], row2a['id'])
        self.assertEqual(row1a['table2'], row2a)
        # Only a's were associated
        self.assertEqual(row1b['table2_id'], None)
        self.assertEqual(row1b['table2'], None)

        # Both sets are associated
        row1b['table2_id'] = row2b['id']
        self.assertEqual(row1a['table2_id'], row2a['id'])
        self.assertEqual(row1a['table2'], row2a)
        self.assertEqual(row1b['table2_id'], row2b['id'])
        self.assertEqual(row1b['table2'], row2b)

        # De-associate a's
        row1a['table2_id'] = None
        self.assertEqual(row1a['table2_id'], None)
        self.assertEqual(row1a['table2'], None)
        # Only b's are associated
        self.assertEqual(row1b['table2_id'], row2b['id'])
        self.assertEqual(row1b['table2'], row2b)

        # Delete has the same effect
        del row1a['table2_id']
        self.assertEqual(row1a['table2_id'], None)
        self.assertEqual(row1a['table2'], None)
        # Only b's are associated
        self.assertEqual(row1b['table2_id'], row2b['id'])
        self.assertEqual(row1b['table2'], row2b)

        # De-associate b's
        row1b['table2_id'] = None
        self.assertEqual(row1a['table2_id'], None)
        self.assertEqual(row1a['table2'], None)
        self.assertEqual(row1b['table2_id'], None)
        self.assertEqual(row1b['table2'], None)

        # Delete has the same effect
        del row1b['table2_id']
        self.assertEqual(row1a['table2_id'], None)
        self.assertEqual(row1a['table2'], None)
        self.assertEqual(row1b['table2_id'], None)
        self.assertEqual(row1b['table2'], None)



if __name__ == '__main__':
    unittest.main()
