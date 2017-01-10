#! /usr/bin/env python
from dictorm import (DictDB, Table, Dict, UnexpectedRows, NoPrimaryKey,
    ResultsGenerator, column_value_pairs)
from pprint import pprint
from psycopg2.extras import DictCursor
import os
import psycopg2
import sqlite3
import unittest

if 'CI' in os.environ.keys():
    test_db_login = {
            'database':'dictorm',
            'user':'postgres',
            'password':'',
            'host':'localhost',
            'port':'5432',
            }
else:
    test_db_login = {
            'database':'dictorm',
            'user':'dictorm',
            'password':'dictorm',
            'host':'localhost',
            'port':'5432',
            }

def _remove_refs(o):
    if type(o) == Dict:
        return o.remove_refs()
    return [i.remove_refs() for i in o]


class ExtraTestMethods(unittest.TestCase):

    def assertDictContains(self, d1, d2):
        assert set(d2.items()).issubset(set(d1.items())), '{} does not contain {}'.format(d1, d2)

    def assertRaisesAny(self, exps, func, a=[], kw={}):
        try:
            func(*a, **kw)
        except Exception as e:
            if type(e) in exps: return
        raise Exception('Did not raise one of the exceptions provided!')



class TestPostgresql(ExtraTestMethods):

    def setUp(self):
        self.conn = psycopg2.connect(**test_db_login)
        self.db = DictDB(self.conn)
        self.curs = self.db.curs
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
        CREATE TABLE station (
            person_id INTEGER
        );
        CREATE TABLE possession (
            id SERIAL PRIMARY KEY,
            person_id INTEGER,
            possession JSONB
        );
        ''')
        self.conn.commit()
        self.db.refresh_tables()


    def tearDown(self):
        self.conn.rollback()
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
        self.assertEqual(list(Person.get_where(1)), [bob,])

        # A second flush does not fail
        bob.flush()
        self.assertDictContains(bob, {'name':'Bob', 'id':1})
        self.assertEqual(list(Person.get_where(1)), [bob,])

        bob['name'] = 'Jon'
        bob.flush()
        self.assertDictContains(bob, {'name':'Jon', 'id':1})
        self.assertEqual(list(Person.get_where(1)), [bob,])

        self.conn.commit()
        self.assertEqual(list(Person.get_where({'id':1})), [bob,])

        # Items are inserted in the order they are flushed
        alice = Person(name='Alice')
        dave = Person(name='Dave')
        dave.flush()
        alice.flush()

        # get_where with a single integer argument should produce a single
        # Dict row that matches that row's id
        self.assertEqual(list(Person.get_where(1)), [bob,])
        self.assertEqual(self.curs.rowcount, 1)

        # get_where with no parameters returns the entire table
        self.assertEqual(list(Person.get_where()), [bob, dave, alice])

        # A delete sql command can be executed on a Dict
        dave.delete()
        self.assertEqual(list(Person.get_where()), [bob, alice])
        self.conn.commit()
        self.assertEqual(list(Person.get_where()), [bob, alice])

        # get_where accepts a tuple of ids, and returns those rows
        self.assertEqual(list(Person.get_where(id=(1,3))),
                [bob, alice])

        # Database row survives an object deletion
        del bob
        del alice
        self.conn.commit()
        self.assertEqual(len(list(Person.get_where())), 2)

        bob, alice = Person.get_where()
        bob.delete()
        alice.delete()
        self.assertEqual(len(list(Person.get_where())), 0)



    def test_get_where_multiple_pks(self):
        Person = self.db['person']
        self.assertEqual(0, Person.count())
        bob = Person(name='Bob')
        bob.flush()

        Department = self.db['department']
        self.assertEqual(0, Department.count())
        sales = Department(name='Sales')
        sales.flush()

        PD = self.db['person_department']
        bob_sales = PD(person_id=bob['id'], department_id=sales['id'])
        bob_sales.flush()
        self.assertEqual(bob_sales['person_id'], bob['id'])
        self.assertEqual(bob_sales['department_id'], sales['id'])
        # Searching person_department with two key/value pairs returns the new
        # row.
        self.assertEqual(
                list(PD.get_where(person_id=1, department_id=1)),
                [bob_sales,])

        # Test deletion with multiple Primary Keys
        bob_sales.delete()
        self.assertEqual(len(list(PD.get_where())), 0)


    def test_already_in_db(self):
        Person = self.db['person']
        self.assertEqual(0, Person.count())
        bob = Person(name='Bob')
        bob.flush()

        bob_copy = Person.get_one(1)
        bob_copy.flush()
        self.assertEqual(bob, bob_copy)


    def test_dict_inits(self):
        Person = self.db['person']
        Person({'name':'Bob'}).flush()
        Person(name='Alice').flush()
        Person([('name','Steve'),]).flush()

        Dict(Person, {'name':'Bob'}).flush()
        Dict(Person, name='Alice').flush()
        Dict(Person, [('name','Steve'),]).flush()

        # A fake column will fail when going into the database
        p = Person(fake_column='foo')
        self.assertRaisesAny((psycopg2.ProgrammingError, sqlite3.OperationalError), p.flush)
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


    def test_manytomany(self):
        """
        Linking to person.id from person_department.person_id allows you to have
        multiple person_department records.

        person              | person_department            | department
        --------------------+------------------------------+-------------------
        id <-------+-+----- | person_id   department_id -> | id
                    \ \---- | person_id   department_id -> | id
                     \----- | person_id   department_id -> | id
        """
        Person = self.db['person']
        Department = self.db['department']
        PD = self.db['person_department']
        PD.sort_by = 'person_id'
        PD['department'] = PD['department_id'] == Department['id']
        PD['person'] = PD['person_id'] == Person['id']
        Person['person_departments'] = Person['id'] > PD['person_id']

        bob = Person(name='Bob')
        bob.flush()
        self.assertDictContains(bob, {'name':'Bob', 'id':1})

        sales = Department(name='Sales')
        sales.flush()
        bob_pd_sales = PD(department_id=sales['id'], person_id=bob['id'])
        bob_pd_sales.flush()
        bob.flush()
        self.assertEqual(list(bob['person_departments']), [bob_pd_sales,])

        hr = Department(name='HR')
        hr.flush()
        bob_pd_hr = PD(department_id=hr['id'], person_id=bob['id'])
        bob_pd_hr.flush()
        bob.flush()
        self.assertEqual(list(bob['person_departments']), [bob_pd_sales, bob_pd_hr])

        aly = Person(name='Aly')
        aly.flush()
        bob.flush()
        self.assertEqual(list(bob['person_departments']), [bob_pd_sales, bob_pd_hr])

        aly_pd_sales = PD(department_id=sales['id'], person_id=aly['id'])
        aly_pd_sales.flush()
        aly.flush()
        self.assertEqual(list(aly['person_departments']), [aly_pd_sales,])
        self.assertEqual(list(bob['person_departments']), [bob_pd_sales, bob_pd_hr])

        # Move bob's hr to aly
        bob_pd_hr['person_id'] = aly['id']
        aly_pd_hr = bob_pd_hr.flush()
        self.assertEqual(_remove_refs(aly['person_departments']), _remove_refs([aly_pd_sales, aly_pd_hr]))
        self.assertEqual(_remove_refs(bob['person_departments']), [bob_pd_sales.remove_refs()])


    def test_substratum_many(self):
        """
        Creating a reference using two other references fascilitates getting
        rows from a third table, if the second table's contents aren't needed
        often, like a join table.
        """
        Person = self.db['person']
        Department = self.db['department']
        PD = self.db['person_department']
        # Setup the initial references
        Person['person_departments'] = Person['id'] > PD['person_id']
        PD['department'] = PD['department_id'] == Department['id']

        # Directly access a person's departments by getting the sub-references
        Person['departments'] = Person['person_departments'].substratum('department')

        # Create the associated rows
        bob = Person(name='Bob').flush()
        # Departments
        sales = Department(name='Sales').flush()
        hr = Department(name='HR').flush()
        # rows linking person and department using join table "person_department"
        bob_pd_sales = PD(department_id=sales['id'], person_id=bob['id']).flush()
        bob_pd_hr = PD(department_id=hr['id'], person_id=bob['id']).flush()

        self.assertEqual(list(bob['departments']), [sales, hr])


    def test_substratum_one(self):
        Person = self.db['person']
        Car = self.db['car']
        # Setup the initial references
        Person['manager'] = Person['id'] == Person['manager_id']
        Person['car'] = Person['car_id'] == Car['id']

        # Directly access a person's manager's car by getting the sub-reference
        Person['manager_car'] = Person['manager'].substratum('car')

        bob = Person(name='Dave').flush()
        alice_car = Car(name='Prius').flush()
        alice = Person(name='Alice', manager_id=bob['id'], car_id=alice_car['id']).flush()

        self.assertEqual(bob['manager_car'].remove_refs(), alice_car.remove_refs())


    def test_onetomany(self):
        """
        person              | car
        --------------------+--------------------------------------------------
        id     <----+-+---- | person_id
                     \ \--- | person_id
                      \---- | person_id
        """
        Person = self.db['person']

        Car = self.db['car']
        Person['cars'] = Person['id'] > Car['person_id']

        bob = Person(name='Bob').flush()

        self.assertEqual(list(bob.get('cars')), [])

        toyota = Car(name='Toyota', person_id=bob['id']).flush()
        honda = Car(name='Honda', person_id=bob['id']).flush()
        ford = Car(name='Ford', person_id=bob['id']).flush()

        self.assertEqual(list(bob.get('cars')), [toyota, honda, ford])
        self.assertEqual(list(bob['cars']), [toyota, honda, ford])


    def test_onetomany_alter_primary_key(self):
        Person = self.db['person']
        bob = Person(name='Bob').flush()
        aly = Person(name='Aly').flush()

        Station = self.db['station']
        Station.order_by = 'person_id'
        Station['person'] = Station['person_id'] == Person['id']
        Person['stations'] = Person['id'] > Station['person_id']

        desk1 = Station(person_id=bob['id']).flush()
        desk2 = Station(person_id=bob['id']).flush()
        desk3 = Station(person_id=bob['id']).flush()

        self.assertEqual(list(bob['stations']), [desk1, desk2, desk3])

        bob.delete()
        self.conn.commit()

        self.assertEqual(desk1['person_id'], 1)
        self.assertEqual(desk2['person_id'], 1)
        self.assertEqual(desk3['person_id'], 1)

        aly['id'] = 1
        aly.flush()
        self.assertEqual(list(aly['stations']), [desk1, desk2, desk3])


    def test_changing_pks(self):
        Person = self.db['person']
        bob = Person(name='Bob')
        bob.flush()
        self.assertEqual(bob['id'], 1)
        bob['id'] = 2
        bob.flush()
        self.assertEqual(bob['id'], 2)


    def test_onetoone(self):
        """
        person              | car
        --------------------+--------------------------------------------------
        id     <----------- | person_id
        car_id -----------> | id
        """
        Person = self.db['person']
        Car = self.db['car']
        Person['car'] = Person['car_id'] == Car['id']
        Car['person'] = Car['person_id'] == Person['id']

        will = Person(name='Will')
        will.flush()
        stratus = Car(name='Dodge Stratus', license_plate='123ABC')
        stratus.flush()
        stratus['person_id'], will['car_id'] = will['id'], stratus['id']
        stratus.flush()
        will.flush()

        self.assertEqual(will.get('car').remove_refs(), stratus.remove_refs())
        self.assertEqual(will['car'].remove_refs(), stratus.remove_refs())
        self.assertEqual(stratus['person'].remove_refs(), will.remove_refs())


    def test_errors(self):
        """
        A table with no primary key(s) can be gotten, but not updated.
        """
        Person = self.db['person']

        bob = Person(name='Bob').flush()
        aly = Person(name='Aly').flush()

        self.assertRaises(UnexpectedRows, Person.get_one)

        bob['not_real_column'] = 1
        self.assertRaisesAny((psycopg2.ProgrammingError, sqlite3.OperationalError), bob.flush)
        self.conn.rollback()

        NoPk = self.db['no_pk']
        foo = NoPk(foo='bar')
        foo.flush()
        self.conn.commit()
        self.assertEqual(foo, {'foo':'bar'})
        self.assertEqual(list(NoPk.get_where()), [{'foo':'bar'},])
        foo['foo'] = 'baz'
        self.assertRaises(NoPrimaryKey, foo.flush)
        self.assertRaises(NoPrimaryKey, NoPk.get_where, 1)


    def test_column_value_pairs(self):
        self.assertEqual(column_value_pairs('postgresql', {'id':10, 'person':'Dave'}),
                'id=%(id)s, person=%(person)s')
        self.assertEqual(column_value_pairs('postgresql', ('id', 'person')),
                'id=%(id)s, person=%(person)s')
        self.assertEqual(column_value_pairs('postgresql', {'id':(10,11,13), 'group':'foo'}, ' AND '),
                'group=%(group)s AND id IN %(id)s')
        self.assertEqual(column_value_pairs('postgresql', {'id':12, 'person':'Dave'}, prefix='old_'),
                'id=%(old_id)s, person=%(old_person)s')


    def test_second_cursor(self):
        """
        Dict's cursor should not interfere with another cursor.
        """
        Person = self.db['person']
        bob = Person(name='Bob')
        bob.flush()
        aly = Person(name='Aly')
        aly.flush()
        self.assertDictContains(bob, {'name':'Bob', 'id':1})

        curs2 = self.conn.cursor(cursor_factory=DictCursor)
        persons = Person.get_where()
        self.assertEqual(next(persons), bob)

        curs2.execute('SELECT * FROM person')
        self.assertEqual(next(persons), aly)

        # Using dictorm's cursor will intefere
        persons = Person.get_where()
        self.assertEqual(next(persons), bob)
        persons.curs.execute('SELECT * FROM person')
        self.assertEqual(next(persons), bob)
        self.assertEqual(next(persons), aly)
        self.assertRaises(StopIteration, next, persons)


    def test_order_by(self):
        Person = self.db['person']
        bob = Person(name='Bob').flush()
        aly = Person(name='Aly').flush()
        wil = Person(name='Wil').flush()

        self.assertEqual(list(Person.get_where()), [bob, aly, wil])
        Person.order_by = 'id asc'
        self.assertEqual(list(Person.get_where()), [bob, aly, wil])
        Person.order_by = 'id desc'
        self.assertEqual(list(Person.get_where()), [wil, aly, bob])

        NoPk = self.db['no_pk']
        NoPk(foo='bar').flush()
        NoPk(foo='baz').flush()
        self.assertEqual(len(list(NoPk.get_where())), 2)
        self.assertNotIn('ORDER BY', NoPk.curs.query.decode())
        NoPk.order_by = 'foo desc'
        results = NoPk.get_where()
        self.assertEqual(len(list(results)), 2)
        self.assertIn('ORDER BY foo desc', results.curs.query.decode())

        NoPk.order_by = None
        self.assertEqual(len(list(NoPk.get_where(foo='bar'))), 1)
        self.assertNotIn('ORDER BY', NoPk.curs.query.decode())
        NoPk.order_by = 'foo desc'
        results = NoPk.get_where(foo='bar')
        self.assertEqual(len(list(results)), 1)
        self.assertIn('ORDER BY foo desc', results.curs.query.decode())


    def test_json(self):
        Possession = self.db['possession']
        p = Possession(possession={'foo':'bar', 'baz':1})
        p.flush()
        self.assertEqual(Possession.get_one()['possession'], {'foo':'bar', 'baz':1})

        # Testing an update of a json
        p['possession'] = {'foo':'baz'}
        p.flush()
        self.assertEqual(Possession.get_one()['possession'], {'foo':'baz'})


    def test_multiple_references(self):
        """
        person               | person
        ---------------------+---------------
        id <---------------- | manager_id


        person               | person
        ---------------------+---------------
        id  <--+-+---------- | manager_id
                \ \--------- | manager_id
                 \---------- | manager_id
        """
        Person = self.db['person']
        Person['manager'] = Person['manager_id'] == Person['id']
        alice = Person(name='Alice').flush()
        self.assertEqual(None, alice['manager'])

        dave = Person(name='Dave', manager_id=alice['id']).flush()
        self.assertDictContains(dave, {'name':'Dave', 'manager_id':1, 'manager':None})
        self.assertEqual(dave['manager'].remove_refs(), alice.remove_refs())
        bob = Person(name='Bob', manager_id=alice['id']).flush()
        self.assertNotEqual(bob['manager'], None)
        self.assertEqual(bob['manager'].remove_refs(), alice.remove_refs())

        # New reference, no flush required
        Person['subordinates'] = Person['id'] > Person['manager_id']
        self.assertEqual(_remove_refs(alice['subordinates']),
                _remove_refs([dave, bob]))

        # Changes survive a commit/flush
        self.conn.commit()
        bob.flush()
        alice.flush()
        dave.flush()
        self.assertEqual(_remove_refs(alice['subordinates']),
                _remove_refs([dave, bob]))
        self.assertEqual(dave['manager'].remove_refs(), alice.remove_refs())
        self.assertEqual(bob['manager'].remove_refs(), alice.remove_refs())

        PD, Department = self.db['person_department'], self.db['department']
        PD['department'] = PD['department_id'] == Department['id']
        Person['person_departments'] = Person['id'] > PD['person_id']

        hr = Department(name='HR').flush()
        sales = Department(name='Sales').flush()
        hr_pd = PD(department_id=hr['id'], person_id=dave['id']).flush()
        sales_pd = PD(department_id=sales['id'], person_id=dave['id']).flush()

        # All references are available on demand
        self.assertEqual(_remove_refs(dave['person_departments']),
                _remove_refs([hr_pd, sales_pd]))
        self.assertEqual(_remove_refs(alice['subordinates']),
                _remove_refs([dave, bob]))
        self.assertEqual(dave['manager'].remove_refs(), alice.remove_refs())
        self.assertEqual(bob['manager'].remove_refs(), alice.remove_refs())

        # You can iterate through subordinates using a for loop
        for sub in alice['subordinates']:
            for pd in sub['person_departments']:
                pd.delete()
            sub.delete()


    def test_count(self):
        """
        Simple reference counting is supported.
        """
        Person = self.db['person']
        Person['subordinates'] = Person['id'] > Person['manager_id']
        alice = Person(name='Alice').flush()
        dave = Person(name='Dave', manager_id=alice['id']).flush()
        bob = Person(name='Bob', manager_id=alice['id']).flush()
        self.assertIsInstance(alice['subordinates'], ResultsGenerator)
        self.assertNotIn(alice._curs.query.decode(), 'SELECT *')
        # get len() without running a larger query
        self.assertEqual(len(alice['subordinates']), 2)
        # you can still get the same old results even after running a len()
        self.assertEqual(_remove_refs(alice['subordinates']),
                _remove_refs([dave, bob]))
        # the generator can be converted to a list
        self.assertEqual(_remove_refs(list(alice['subordinates'])),
                _remove_refs([dave, bob]))

        subs = alice['subordinates']
        self.assertEqual(len(subs), 2)
        self.assertEqual(_remove_refs(subs), _remove_refs([dave, bob]))


    def test_empty_reference(self):
        """
        Iterating through an empty reference does not break.
        """
        Person = self.db['person']
        Person['subordinates'] = Person['id'] > Person['manager_id']
        alice = Person(name='Alice').flush()

        self.assertEqual(len(alice['subordinates']), 0)
        for sub in alice['subordinates']:
            raise Exception('There should not be any subordinates')

        Person['manager'] = Person['id'] == Person['manager_id']
        self.assertEqual(alice['manager'], None)



class TestSqlite(TestPostgresql):

    def assertDictContains(self, d1, d2):
        assert set(d2.items()).issubset(set(d1.items())), '{} does not contain {}'.format(d1, d2)


    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.db = DictDB(self.conn)
        self.curs = self.db.curs
        self.tearDown()
        self.curs.executescript('''
        CREATE TABLE person (
            id INTEGER PRIMARY KEY,
            name TEXT,
            manager_id INTEGER REFERENCES person(id)
        );
        CREATE TABLE department (
            id INTEGER PRIMARY KEY,
            name TEXT
        );
        CREATE TABLE person_department (
            person_id INTEGER REFERENCES person(id),
            department_id INTEGER REFERENCES department(id),
            PRIMARY KEY (person_id, department_id)
        );
        CREATE TABLE car (
            id INTEGER PRIMARY KEY,
            license_plate TEXT,
            name TEXT,
            person_id INTEGER REFERENCES person(id)
        );
        ALTER TABLE person ADD COLUMN car_id INTEGER REFERENCES car(id);
        CREATE TABLE no_pk (foo TEXT);
        CREATE TABLE station (
            person_id INTEGER
        );
        CREATE TABLE possession (
            id INTEGER PRIMARY KEY,
            person_id INTEGER,
            possession JSONB
        );
        ''')
        self.conn.commit()
        self.db.refresh_tables()


    def tearDown(self):
        self.conn.rollback()
        self.curs.execute("""select 'drop table ' || name || ';' from
                sqlite_master where type = 'table';""")
        self.conn.commit()


    def test_get_where(self):
        Person = self.db['person']
        self.assertEqual(0, Person.count())

        bob = Person(name='Bob')
        self.assertEqual({'name':'Bob'}, bob)
        bob.flush()
        self.assertDictContains(bob, {'name':'Bob', 'id':1})
        self.assertEqual(list(Person.get_where(1)), [bob,])

        # A second flush does not fail
        bob.flush()
        self.assertDictContains(bob, {'name':'Bob', 'id':1})
        self.assertEqual(list(Person.get_where(1)), [bob,])

        bob['name'] = 'Jon'
        bob.flush()
        self.assertDictContains(bob, {'name':'Jon', 'id':1})
        self.assertEqual(list(Person.get_where(1)), [bob,])

        self.conn.commit()
        self.assertEqual(list(Person.get_where({'id':1})), [bob,])

        # Items are inserted in the order they are flushed
        alice = Person(name='Alice')
        dave = Person(name='Dave')
        dave.flush()
        alice.flush()

        # get_where with a single integer argument should produce a single
        # Dict row that matches that row's id
        self.assertEqual(list(Person.get_where(1)), [bob,])

        # get_where with no parameters returns the entire table
        self.assertEqual(list(Person.get_where()), [bob, dave, alice])

        # A delete sql command can be executed on a Dict
        dave.delete()
        self.assertEqual(list(Person.get_where()), [bob, alice])
        self.conn.commit()
        self.assertEqual(list(Person.get_where()), [bob, alice])

        # get_where accepts a tuple of ids, and returns those rows
        self.assertEqual(list(Person.get_where(id=(1,3))),
                [bob, alice])

        # Database row survives an object deletion
        del bob
        del alice
        self.conn.commit()
        self.assertEqual(len(list(Person.get_where())), 2)

        bob, alice = Person.get_where()
        bob.delete()
        alice.delete()
        self.assertEqual(len(list(Person.get_where())), 0)



    def test_order_by(self):
        Person = self.db['person']
        bob = Person(name='Bob').flush()
        aly = Person(name='Aly').flush()
        wil = Person(name='Wil').flush()

        self.assertEqual(list(Person.get_where()), [bob, aly, wil])
        Person.order_by = 'id asc'
        self.assertEqual(list(Person.get_where()), [bob, aly, wil])
        Person.order_by = 'id desc'
        self.assertEqual(list(Person.get_where()), [wil, aly, bob])

        NoPk = self.db['no_pk']
        NoPk(foo='bar').flush()
        NoPk(foo='baz').flush()
        self.assertEqual(len(list(NoPk.get_where())), 2)
        NoPk.order_by = 'foo desc'
        results = list(NoPk.get_where())
        self.assertEqual(len(results), 2)
        self.assertEqual(results, [{'foo': 'baz'}, {'foo': 'bar'}])

        NoPk.order_by = 'foo asc'
        results = list(NoPk.get_where())
        self.assertEqual(len(results), 2)
        self.assertEqual(results, [{'foo': 'bar'}, {'foo': 'baz'}])

        NoPk.order_by = None
        self.assertEqual(len(list(NoPk.get_where(foo='bar'))), 1)
        NoPk.order_by = 'foo desc'
        results = list(NoPk.get_where(foo='bar'))
        self.assertEqual(len(results), 1)
        self.assertEqual(results, [{'foo': 'bar'},])


    def test_column_value_pairs(self):
        self.assertEqual(column_value_pairs('sqlite3',
            {'id':10, 'person':'Dave'}),
                'id=:id, person=:person')
        self.assertEqual(column_value_pairs('sqlite3', ('id', 'person')),
                'id=:id, person=:person')
        self.assertEqual(column_value_pairs('sqlite3',
            {'id':(10,11,13), 'group':'foo'}, ' AND '),
                'group=:group AND id IN (10,11,13)')
        self.assertEqual(column_value_pairs('sqlite3',
            {'id':12, 'person':'Dave'}, prefix='old_'),
                'id=:old_id, person=:old_person')
        self.assertRaises(ValueError, column_value_pairs,
                'sqlite3', {'id':(10,11,'foo'), 'group':'foo'})


    # Not supported for sqlite
    test_count = None
    test_json = None
    test_second_cursor = None



if __name__ == '__main__':
    unittest.main()
