#! /usr/bin/env python
from pgpydict import *
from pprint import pprint
from psycopg2 import OperationalError
from psycopg2.extras import DictCursor
import os
import psycopg2
import unittest

if 'CI' in os.environ.keys():
    test_db_login = {
            'database':'pgpydict',
            'user':'postgres',
            'password':'',
            'host':'localhost',
            'port':'5432',
            }
else:
    test_db_login = {
            'database':'pgpydict',
            'user':'pgpydict',
            'password':'pgpydict',
            'host':'localhost',
            'port':'5432',
            }

class Test(unittest.TestCase):

    def assertDictContains(self, d1, d2):
        assert set(d2.items()).issubset(set(d1.items())), '{} does not contain {}'.format(d1, d2)


    def setUp(self):
        self.conn = psycopg2.connect(**test_db_login)
        self.curs = self.conn.cursor(cursor_factory=DictCursor)
        self.tearDown()
        self.curs.execute('''
        CREATE TABLE person (
            id SERIAL PRIMARY KEY,
            name TEXT,
            manager_id INTEGER REFERENCES person(id)
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
        CREATE TABLE car (
            id SERIAL PRIMARY KEY,
            license_plate TEXT,
            name TEXT,
            person_id INTEGER REFERENCES person(id)
        );
        ALTER TABLE person ADD COLUMN car_id INTEGER REFERENCES car(id);
        CREATE TABLE no_pk (foo TEXT);
        ''')
        self.conn.commit()
        self.db = DictDB(self.curs)


    def tearDown(self):
        self.curs.execute('''DROP SCHEMA public CASCADE;
                CREATE SCHEMA public;
                GRANT ALL ON SCHEMA public TO postgres;
                GRANT ALL ON SCHEMA public TO public;''')
        self.conn.commit()


    def test_DictDB(self):
        self.db.refresh_tables()


    def test_get_where(self):
        Person = self.db['person']
        self.assertEqual(0, Person.count())

        bob = Person(name='Bob')
        self.assertEqual({'name':'Bob'}, bob)
        bob.flush()
        self.assertDictContains(bob, {'name':'Bob', 'id':1})
        self.assertEqual(Person.get_where(1), bob)

        # A second flush does not fail
        bob.flush()
        self.assertDictContains(bob, {'name':'Bob', 'id':1})
        self.assertEqual(Person.get_where(1), bob)

        bob['name'] = 'Jon'
        bob.flush()
        self.assertDictContains(bob, {'name':'Jon', 'id':1})
        self.assertEqual(Person.get_where(1), bob)

        self.conn.commit()
        self.assertEqual(Person.get_where(1), bob)

        # Items are inserted in the order they are flushed
        alice = Person(name='Alice')
        dave = Person(name='Dave')
        dave.flush()
        alice.flush()

        # get_where with no parameters returns the entire table
        self.assertEqual(Person.get_where(), [bob, dave, alice])

        # A delete sql command can be executed on a PgPyDict
        dave.delete()
        self.assertEqual(Person.get_where(), [bob, alice])
        self.conn.commit()
        self.assertEqual(Person.get_where(), [bob, alice])

        # Database row survives an object deletion
        del bob
        del alice
        self.conn.commit()
        self.assertEqual(len(Person.get_where()), 2)



    def test_get_where_multiple_pks(self):
        Person = self.db['person']
        self.assertEqual(0, Person.count())
        bob = Person(name='Bob')
        bob.flush()

        Department = self.db['department']
        self.assertEqual(0, Department.count())
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
                PersonDepartment.get_where(person_id=1, department_id=1),
                bob_sales)

        # Test deletion with multiple Primary Keys
        bob_sales.delete()
        self.assertRaises(NoEntryError, PersonDepartment.get_where)


    def test_already_in_db(self):
        Person = self.db['person']
        self.assertEqual(0, Person.count())
        bob = Person(name='Bob')
        bob.flush()

        bob_copy = Person.get_where(1)
        bob_copy.flush()
        self.assertEqual(bob, bob_copy)


    def test_dict_inits(self):
        Person = self.db['person']
        Person({'name':'Bob'}).flush()
        Person(name='Alice').flush()
        Person([('name','Steve'),]).flush()

        PgPyDict(Person, {'name':'Bob'}).flush()
        PgPyDict(Person, name='Alice').flush()
        PgPyDict(Person, [('name','Steve'),]).flush()

        # A fake column will fail when going into the database
        p = Person(fake_column='foo')
        self.assertRaises(psycopg2.ProgrammingError, p.flush)
        self.conn.rollback()



    def test_remove_pks(self):
        Person = self.db['person']
        self.assertEqual(0, Person.count())
        bob = Person(name='Bob')
        self.assertEqual(bob, {'name':'Bob'})
        bob.flush()
        self.assertDictContains(bob, {'name':'Bob', 'id':1})
        self.assertDictContains(bob.remove_pks(), {'name':'Bob'})

        aly = Person(name='Aly')
        self.assertEqual(aly, {'name':'Aly'})
        aly.flush()
        self.assertDictContains(aly, {'name':'Aly', 'id':2})
        self.assertDictContains(aly.remove_pks(), {'name':'Aly'})

        bob.update(aly.remove_pks())
        bob.flush()
        aly.flush()
        self.assertDictContains(bob, {'name':'Aly', 'id':1})
        self.assertDictContains(aly, {'name':'Aly', 'id':2})


    def test_set_reference(self):
        """
        person
        ════════════════════
        id <----------\
        manager_id ---/
        """
        Person = self.db['person']
        Person.set_reference('manager_id', 'manager', Person, 'id')

        bob = Person(name='Bob')
        bob.flush()
        self.assertDictContains(bob, {'name':'Bob', 'id':1})
        aly = Person(name='Aly')
        aly.flush()

        bob['manager_id'] = aly['id']
        bob.flush()
        self.assertEqual(bob['manager_id'], aly['id'])
        self.assertEqual(bob['manager'], aly)

        steve = Person(name='Steve')
        steve.flush()
        bob['manager_id'] = steve['id']
        self.assertEqual(bob['manager_id'], steve['id'])
        self.assertEqual(bob['manager'], steve)

        bob['manager'] = aly
        bob.flush()
        self.assertEqual(bob['manager_id'], aly['id'])
        self.assertEqual(bob['manager'], aly)


    def test_onetomany(self):
        """
        Linking to person.id from person_department.person_id allows you to have
        multiple person_department records.

        person              ║ person_department            ║ department
        ════════════════════╬══════════════════════════════╬════════════════════
        id <-------+-+----- ║ person_id   department_id -> ║ id
                    \ \---- ║ person_id   department_id -> ║ id
                     \----- ║ person_id   department_id -> ║ id
        """
        Person = self.db['person']
        Department = self.db['department']
        PD = self.db['person_department']
        PD.sort_by = 'person_id'
        PD.set_reference('department_id', 'department', Department, 'id')
        PD.set_reference('person_id', 'person', Person, 'id')
        Person.set_reference('id', 'person_departments', PD, 'person_id', is_list=True)

        bob = Person(name='Bob')
        bob.flush()
        self.assertDictContains(bob, {'name':'Bob', 'id':1})

        sales = Department(name='Sales')
        sales.flush()
        bob_pd_sales = PD(department_id=sales['id'], person_id=bob['id'])
        bob_pd_sales.flush()
        bob.flush()
        self.assertEqual(bob['person_departments'], [bob_pd_sales,])

        hr = Department(name='HR')
        hr.flush()
        bob_pd_hr = PD(department_id=hr['id'], person_id=bob['id'])
        bob_pd_hr.flush()
        bob.flush()
        self.assertEqual(bob['person_departments'], [bob_pd_sales, bob_pd_hr])

        aly = Person(name='Aly')
        aly.flush()
        bob.flush()
        self.assertEqual(bob['person_departments'], [bob_pd_sales, bob_pd_hr])

        aly_pd_sales = PD(department_id=sales['id'], person_id=aly['id'])
        aly_pd_sales.flush()
        aly.flush()
        self.assertEqual(aly['person_departments'], [aly_pd_sales,])
        self.assertEqual(bob['person_departments'], [bob_pd_sales, bob_pd_hr])


    def test_onetoone(self):
        """
        person              ║ car
        ════════════════════╬═══════════════════════════════════════════════════
        id     <----------- ║ person_id
        car_id -----------> ║ id
        """
        Person = self.db['person']
        Car = self.db['car']
        Person.set_reference('car_id', 'car', Car, 'id')
        Car.set_reference('person_id', 'person', Person, 'id')

        will = Person(name='Will')
        will.flush()
        stratus = Car(name='Dodge Stratus', license_plate='123ABC')
        stratus.flush()
        stratus['person_id'], will['car_id'] = will['id'], stratus['id']
        stratus.flush()
        will.flush()

        self.assertEqual(will['car'].remove_refs(), stratus.remove_refs())
        self.assertEqual(stratus['person'].remove_refs(), will.remove_refs())


    def test_errors(self):
        """
        Getting a bad primary key raises NoEntryError.

        A table with no primary key(s) can be gotten, but not updated.
        """
        Person = self.db['person']
        self.assertRaises(NoEntryError, Person.get_where, 1)

        NoPk = self.db['no_pk']
        foo = NoPk(foo='bar')
        foo.flush()
        self.assertEqual(foo, {'foo':'bar'})
        self.assertEqual(NoPk.get_where(), {'foo':'bar'})
        foo['foo'] = 'baz'
        self.assertRaises(NoPrimaryKey, foo.flush)



if __name__ == '__main__':
    unittest.main()
