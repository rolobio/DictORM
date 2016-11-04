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
        self.curs.execute('CREATE TABLE table1 (id SERIAL PRIMARY KEY, foo TEXT)')
        self.curs.execute('''CREATE TABLE table2 (
                id SERIAL,
                group_id INTEGER,
                foo TEXT,
                PRIMARY KEY(id, group_id)
            )''')
        self.curs.execute('CREATE TABLE table3 (id INTEGER, foo TEXT)')
        self.conn.commit()


    def tearDown(self):
        self.conn.rollback()
        self.curs.execute('''DROP SCHEMA public CASCADE;
                CREATE SCHEMA public;
                GRANT ALL ON SCHEMA public TO postgres;
                GRANT ALL ON SCHEMA public TO public;''')
        self.conn.commit()


    def test_init(self):
        Table1 = PgPyTable('table1', self.curs)
        row = Table1({'foo':'bar',})
        row_copy = Table1.getByPrimary(1)
        self.assertEqual(row, {'id':1, 'foo':'bar'})
        self.assertEqual(row, row_copy)
        self.assertRaises(psycopg2.IntegrityError, Table1, row)


    def test_multiple_pks(self):
        Table2 = PgPyTable('table2', self.curs)
        row1 = Table2({'group_id':3, 'foo':'bar',})
        row2 = Table2({'group_id':3, 'foo':'bar',})
        self.assertRaises(psycopg2.IntegrityError, Table2, row2)


    def test_getByPrimary_with_multiple_pks(self):
        Table2 = PgPyTable('table2', self.curs)
        a = Table2({'group_id':3, 'foo':'bar',})
        b = Table2.getByPrimary({'id':1, 'group_id':3})
        self.assertEqual(a, b)


    def test_empty(self):
        Table1 = PgPyTable('table1', self.curs)
        Table1({})


    def test_no_pks(self):
        Table3 = PgPyTable('table3', self.curs)
        row = Table3({})
        self.assertEqual(row, {'id':None, 'foo':None})
        row['id'] = 12
        row['foo'] = 'asdf'
        self.assertEqual(row, {'id':12, 'foo':'asdf'})
        self.conn.commit()
        self.assertEqual(row, {'id':12, 'foo':'asdf'})
        self.assertRaises(ValueError, Table3.getByPrimary, 12)



class TestPgPyDict(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect(**test_db_login)
        self.curs = self.conn.cursor(cursor_factory=DictCursor)
        self.tearDown()
        self.curs.execute('CREATE TABLE table1 (id SERIAL PRIMARY KEY, foo TEXT)')
        self.curs.execute('CREATE TABLE table2 (id SERIAL, group_id SERIAL, person TEXT, PRIMARY KEY(id, group_id))')
        self.conn.commit()


    def tearDown(self):
        self.conn.rollback()
        self.curs.execute('''DROP SCHEMA public CASCADE;
                CREATE SCHEMA public;
                GRANT ALL ON SCHEMA public TO postgres;
                GRANT ALL ON SCHEMA public TO public;''')
        self.conn.commit()


    def test___setitem__(self):
        Table1 = PgPyTable('table1', self.curs)
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
        Table1 = PgPyTable('table1', self.curs)
        Table1({'foo':'bar',})
        row = Table1.getByPrimary(1)
        del row['foo']
        self.assertEqual(row['foo'], None)


    def test_update(self):
        """
        Update should change all values except primary keys.
        """
        Table1 = PgPyTable('table1', self.curs)
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
        Table1 = PgPyTable('table1', self.curs)
        row = Table1()
        self.assertEqual(row['id'], 1)


    def test_dict(self):
        """
        Test that the PgPyDict functions as a normal dict.
        """
        Table1 = PgPyTable('table1', self.curs)
        row = Table1({'foo':'bar',})
        self.assertEqual(len(row), 2)
        self.assertEqual(sorted(iter(row)), sorted(['id', 'foo']))

        # Dict can be created by a list of keys and list of values
        row = Table1(zip(('foo',), ('baz',)))
        self.assertEqual(len(row), 2)
        self.assertEqual(sorted(iter(row)), sorted(['id', 'foo']))


    def test_multiple_primarys(self):
        Table2 = PgPyTable('table2', self.curs)
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
        Table1a = PgPyTable('table1', curs1)
        Table1b = PgPyTable('table1', curs2)
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
        Table1 = PgPyTable('table1', self.curs)
        Table2 = PgPyTable('table2', self.curs)
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
        Table1 = PgPyTable('table1', self.curs)
        Table2 = PgPyTable('table2', self.curs)
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
        Table1 = PgPyTable('table1', self.curs)
        Table2 = PgPyTable('table2', self.curs)
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


    def test_sub_dict_update(self):
        Table1 = PgPyTable('table1', self.curs)
        Table2 = PgPyTable('table2', self.curs)
        row2a = Table2({'person':'Dave',})
        row2b = Table2({'person':'Bob',})
        row1a = Table1({'foo':'bar',})
        row1b = Table1({'foo':'baz',})
        # Rows have not yet been associated
        self.assertEqual(row1a['table2_id'], None)
        self.assertEqual(row1a['table2'], None)
        self.assertEqual(row1b['table2_id'], None)
        self.assertEqual(row1b['table2'], None)

        row1a['table2'] = row2a
        self.assertEqual(row1a['table2_id'], row2a['id'])
        self.assertEqual(row1a['table2'], row2a)
        self.assertEqual(row1b['table2_id'], None)
        self.assertEqual(row1b['table2'], None)

        # Setting 1b to 2a using update, both should now be associated with 2a
        row1b.update(row1a)
        self.assertEqual(row1a['table2_id'], row2a['id'])
        self.assertEqual(row1a['table2'], row2a)
        self.assertEqual(row1b['table2_id'], row2a['id'])
        self.assertEqual(row1b['table2'], row2a)

        # Switching 1a to 2b, 1b should still be 2a
        row1a['table2'] = row2b
        self.assertEqual(row1a['table2_id'], row2b['id'])
        self.assertEqual(row1a['table2'], row2b)
        self.assertEqual(row1b['table2_id'], row2a['id'])
        self.assertEqual(row1b['table2'], row2a)

        # Updating 1b again, it should match with 2b
        row1b.update(row1a)
        self.assertEqual(row1a['table2_id'], row2b['id'])
        self.assertEqual(row1a['table2'], row2b)
        self.assertEqual(row1b['table2_id'], row2b['id'])
        self.assertEqual(row1b['table2'], row2b)


    def test_sub_dict_modification(self):
        Table1 = PgPyTable('table1', self.curs)
        Table2 = PgPyTable('table2', self.curs)
        row2 = Table2({'person':'Alice',})
        row1 = Table1({'foo':'bar',})
        row1['table2'] = row2

        row1['table2']['person'] = 'Thomas'
        self.assertEqual(row1['table2']['person'], 'Thomas')
        self.assertEqual(row2['person'], 'Thomas')
        self.conn.commit()
        self.assertEqual(row1['table2']['person'], 'Thomas')
        self.assertEqual(row2['person'], 'Thomas')

        row1 = Table1.getByPrimary(1)
        row2 = Table2.getByPrimary(1)
        self.assertEqual(row1['table2']['person'], 'Thomas')
        self.assertEqual(row2['person'], 'Thomas')


    def test_sub_init(self):
        """
        A sub-dict can be created when creating a pgpydict.
        """
        Table1 = PgPyTable('table1', self.curs)
        Table2 = PgPyTable('table2', self.curs)
        row2_dict = {'person':'Alice'}
        row1 = Table1({'foo':'bar', 'table2':row2_dict})
        row2 = row1['table2']
        # an id as been added by the init
        row2_dict['id'] = row2['id']
        self.assertEqual(row2, row2_dict)
        self.assertEqual(row2, Table2.getByPrimary(1))
        self.conn.commit()
        self.assertEqual(row2, row2_dict)
        self.assertEqual(row2, Table2.getByPrimary(1))



if __name__ == '__main__':
    unittest.main()
