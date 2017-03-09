#! /usr/bin/env python
from dictorm import (DictDB, Table, Dict, UnexpectedRows, NoPrimaryKey,
    ResultsGenerator, Select)
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
    if isinstance(o, Dict):
        return o.no_refs()
    return [i.no_refs() for i in o]


def error(*a, **kw): raise Exception()


class ExtraTestMethods(unittest.TestCase):

    @classmethod
    def assertDictContains(cls, d1, d2):
        if not set(d2.items()).issubset(set(d1.items())):
            raise TypeError('{} does not contain {}'.format(d1, d2))

    @classmethod
    def assertRaisesAny(cls, exps, func, a=None, kw=None):
        a = a or []
        kw = kw or {}
        try:
            func(*a, **kw)
        except Exception as e:
            if isinstance(e, exps): return
        raise Exception('Did not raise one of the exceptions provided!')

    @classmethod
    def assertType(cls, a, b):
        if not isinstance(a, b):
            raise TypeError('{} is not type {}'.format(str(a), b0))


class PostgresTestBase(ExtraTestMethods):

    def setUp(self):
        self.conn = psycopg2.connect(**test_db_login)
        self.db = DictDB(self.conn)
        self.curs = self.db.curs
        self.tearDown()
        self.curs.execute('''
        CREATE TABLE person (
            id SERIAL PRIMARY KEY,
            name TEXT,
            other INTEGER,
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
            description JSONB
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




class TestPostgresql(PostgresTestBase):

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


    def test_empty(self):
        Person = self.db['person']
        p = Person().flush()
        self.assertEqual(p['id'], 1)


    def test_delete(self):
        Person = self.db['person']
        bob = Person(name='Bob').flush()
        dave = Person(name='Dave').flush()
        alice = Person(name='Alice').flush()

        # A delete sql command can be executed on a Dict
        dave.delete()
        self.assertEqual(list(Person.get_where()), [bob, alice])
        self.conn.commit()
        self.assertEqual(list(Person.get_where()), [bob, alice])

        # get_where accepts a tuple of ids, and returns those rows
        self.assertEqual(list(Person.get_where(Person['id'].In([1,3]))),
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
        bob = Person(name='Bob').flush()

        Department = self.db['department']
        self.assertEqual(0, Department.count())
        sales = Department(name='Sales').flush()

        PD = self.db['person_department']
        bob_sales = PD(person_id=bob['id'], department_id=sales['id']).flush()
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
        bob = Person(name='Bob').flush()

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
        self.assertDictContains(bob.no_pks(), {'name':'Bob'})

        aly = Person(name='Aly')
        self.assertEqual(aly, {'name':'Aly'})
        aly.flush()
        self.assertDictContains(aly, {'name':'Aly', 'id':2})
        self.assertDictContains(aly.no_pks(), {'name':'Aly'})

        bob.update(aly.no_pks())
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
        Person['person_departments'] = Person['id'].many(PD['person_id'])

        bob = Person(name='Bob').flush()
        self.assertDictContains(bob, {'name':'Bob', 'id':1})

        sales = Department(name='Sales').flush()
        bob_pd_sales = PD(department_id=sales['id'], person_id=bob['id']).flush()
        self.assertEqual(list(bob['person_departments']), [bob_pd_sales,])

        hr = Department(name='HR').flush()
        bob_pd_hr = PD(department_id=hr['id'], person_id=bob['id']).flush()
        self.assertEqual(list(bob['person_departments']), [bob_pd_sales, bob_pd_hr])

        # Adding another person doesn't break the list
        aly = Person(name='Aly').flush()
        self.assertEqual(list(bob['person_departments']), [bob_pd_sales, bob_pd_hr])

        aly_pd_sales = PD(department_id=sales['id'], person_id=aly['id']).flush()
        aly.flush()
        self.assertEqual(list(aly['person_departments']), [aly_pd_sales,])
        self.assertEqual(list(bob['person_departments']), [bob_pd_sales, bob_pd_hr])

        # Move bob's hr to aly
        bob_pd_hr['person_id'] = aly['id']
        aly_pd_hr = bob_pd_hr.flush()
        self.assertEqual(_remove_refs(aly['person_departments']), _remove_refs([aly_pd_sales, aly_pd_hr]))
        self.assertEqual(_remove_refs(bob['person_departments']), [bob_pd_sales.no_refs()])


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
        Person['person_departments'] = Person['id'].many(PD['person_id'])
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

        bob = Person(name='Bob').flush()
        alice_car = Car(name='Prius').flush()
        alice = Person(name='Alice', manager_id=bob['id'], car_id=alice_car['id']).flush()

        self.assertEqual(bob['manager_car'].no_refs(), alice_car.no_refs())


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
        Person['cars'] = Person['id'].many(Car['person_id'])

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
        Person['stations'] = Person['id'].many(Station['person_id'])

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
        bob = Person(name='Bob').flush()
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

        will = Person(name='Will').flush()
        stratus = Car(name='Dodge Stratus', license_plate='123ABC').flush()
        stratus['person_id'], will['car_id'] = will['id'], stratus['id']
        stratus.flush()
        will.flush()

        self.assertEqual(will.get('car').no_refs(), stratus.no_refs())
        self.assertEqual(will['car'].no_refs(), stratus.no_refs())
        self.assertEqual(stratus['person'].no_refs(), will.no_refs())


    def test_onetoself(self):
        """
        person              | person
        --------------------+--------------------------------------------------
        id     <----------- | manager_id
        """
        Person = self.db['person']
        Person['manager'] = Person['manager_id'] == Person['id']
        alice = Person(name='Alice').flush()
        bob = Person(name='Bob', manager_id=alice['id']).flush()
        self.assertEqual(bob['manager'], alice)

        bob['manager_id'] = bob['id']
        bob.flush()
        self.assertEqual(bob['manager'].no_refs(), bob.no_refs())


    def test_errors(self):
        """
        A table with no primary key(s) can be gotten, but not updated.
        """
        Person = self.db['person']

        bob = Person(name='Bob').flush()
        aly = Person(name='Aly').flush()

        self.assertRaises(KeyError, bob.__getitem__, 'foo')

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


    def test_second_cursor(self):
        """
        Dict's cursor should not interfere with another cursor.
        """
        Person = self.db['person']
        bob = Person(name='Bob').flush()
        aly = Person(name='Aly').flush()
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
        p = Possession(description={'foo':'bar', 'baz':1}).flush()
        self.assertEqual(Possession.get_one()['description'], {'foo':'bar', 'baz':1})

        # Testing an update of a json
        p['description'] = {'foo':'baz'}
        p.flush()
        self.assertEqual(Possession.get_one()['description'], {'foo':'baz'})


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
        self.assertEqual(dave['manager'].no_refs(), alice.no_refs())
        bob = Person(name='Bob', manager_id=alice['id']).flush()
        self.assertNotEqual(bob['manager'], None)
        self.assertEqual(bob['manager'].no_refs(), alice.no_refs())

        # New reference, no flush required
        Person['subordinates'] = Person['id'].many(Person['manager_id'])
        self.assertEqual(_remove_refs(alice['subordinates']),
                _remove_refs([dave, bob]))

        # Changes survive a commit/flush
        self.conn.commit()
        bob.flush()
        alice.flush()
        dave.flush()
        self.assertEqual(_remove_refs(alice['subordinates']),
                _remove_refs([dave, bob]))
        self.assertEqual(dave['manager'].no_refs(), alice.no_refs())
        self.assertEqual(bob['manager'].no_refs(), alice.no_refs())

        PD, Department = self.db['person_department'], self.db['department']
        PD['department'] = PD['department_id'] == Department['id']
        Person['person_departments'] = Person['id'].many(PD['person_id'])

        hr = Department(name='HR').flush()
        sales = Department(name='Sales').flush()
        hr_pd = PD(department_id=hr['id'], person_id=dave['id']).flush()
        sales_pd = PD(department_id=sales['id'], person_id=dave['id']).flush()

        # All references are available on demand
        self.assertEqual(_remove_refs(dave['person_departments']),
                _remove_refs([hr_pd, sales_pd]))
        self.assertEqual(_remove_refs(alice['subordinates']),
                _remove_refs([dave, bob]))
        self.assertEqual(dave['manager'].no_refs(), alice.no_refs())
        self.assertEqual(bob['manager'].no_refs(), alice.no_refs())

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
        Person['subordinates'] = Person['id'].many(Person['manager_id'])
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
        Person['subordinates'] = Person['id'].many(Person['manager_id'])
        alice = Person(name='Alice').flush()

        self.assertEqual(len(alice['subordinates']), 0)
        self.assertEqual(len(iter(alice['subordinates'])), 0)

        Person['manager'] = Person['id'] == Person['manager_id']
        self.assertEqual(alice['manager'], None)


    def test_reexecute(self):
        """
        References are only gotten once, until they are changed.
        """
        Person = self.db['person']
        Person['manager'] = Person['manager_id'] == Person['id']

        bob = Person(name='Bob').flush()
        alice = Person(name='Alice', manager_id=bob['id']).flush()
        self.assertEqual(alice['manager'], bob)

        original_get_where = alice._table.get_where
        alice._table.get_where = error
        self.assertEqual(alice['manager'], bob)

        steve = Person(name='Steve').flush()

        alice._table.get_where = original_get_where
        alice['manager_id'] = steve['id']
        alice.flush()
        self.assertEqual(alice['manager'].no_refs(), steve.no_refs())


    def test_modify_subdict(self):
        Person = self.db['person']
        Car = self.db['car']
        Person['car'] = Person['car_id'] == Car['id']

        will = Person(name='Will').flush()
        stratus = Car(name='Stratus').flush()
        will['car_id'] = stratus['id']

        will['car']['license_plate'] = 'foo'
        # Flush will, this should also flush car
        will.flush()

        # Get another copy of car
        stratus2 = Car.get_one()
        self.assertEqual(stratus2['license_plate'], 'foo')
        self.assertNotEqual(stratus, stratus2)

        # Flushing the original object overwrites the copy's changes
        stratus.flush()
        self.assertNotEqual(stratus['license_plate'], 'foo')
        self.assertNotEqual(stratus, stratus2)


    def test_table_equal(self):
        """
        A Dicts hidden _table can be compared to itself or other tables.
        """
        Person = self.db['person']
        self.assertEqual(Person, self.db['person'])
        self.assertIs(Person, self.db['person'])

        will = Person(name='Will').flush()
        bob = Person(name='Bob').flush()
        self.assertEqual(will._table, bob._table)
        self.assertIs(will._table, bob._table)

        Car = self.db['car']
        self.assertNotEqual(Person, Car)

        Person['car'] = Person['car_id'] == Car['id']
        stratus = Car(name='Stratus').flush()
        will['car_id'] = stratus['id']
        will.flush()
        will['car']['license_plate'] = 'foo'

        self.assertEqual(stratus._table, Car)
        self.assertIs(stratus._table, Car)
        self.assertEqual(will['car']._table, Car)
        self.assertIs(will['car']._table, Car)


    def test_real(self):
        """
        An attempt at a real-world example.
        """
        Person, Car = self.db['person'], self.db['car']
        PD, Department = self.db['person_department'], self.db['department']
        Possession = self.db['possession']

        Person['manager'] = Person['manager_id'] == Person['id']
        Person['subordinates'] = Person['id'].many(Person['manager_id'])
        Person['person_departments'] = Person['id'].many(PD['person_id'])
        Person['departments'] = Person['person_departments'].substratum('department')
        Person['car'] = Person['car_id'] == Car['id']
        Person['possessions'] = Person['id'].many(Possession['person_id'])
        Car['person'] = Car['person_id'] == Person['id']
        PD['person'] = Person['id'] == PD['person_id']
        PD['department'] =  PD['department_id'] == Department['id']
        Department['person_departments'] = Department['id'].many(PD['department_id'])
        Department['persons'] = Department['person_departments'].substratum('person')
        Possession['person'] = Possession['person_id'] == Person['id']

        # Milton has a car
        milton = Person(name='Milton').flush()
        miltons_car = Car(name='Ford', person_id=milton['id']).flush()
        milton['car_id'] = miltons_car['id']
        sales = Department(name='Sales').flush()
        self.assertEqual(milton['car'].no_refs(), miltons_car.no_refs())
        milton.flush()
        miltons_car.flush()
        self.assertEqual(milton['car'], miltons_car)

        # Milton is in Sales
        milton_sales = PD(person_id=milton['id'], department_id=sales['id']).flush()
        self.assertEqual(milton_sales, PD.get_one())
        self.assertEqual(milton_sales['person'].no_refs(), milton.no_refs())
        self.assertEqual(milton_sales['department'].no_refs(), sales.no_refs())
        self.assertEqual(_remove_refs(milton['departments']), _remove_refs([sales,]))
        self.assertEqual(_remove_refs(sales['persons']), [milton.no_refs(),])

        # Milton has a stapler
        miltons_stapler = Possession(person_id=milton['id'],
                description={'kind':'stapler', 'brand':'Swingline', 'color':'Red'}
                ).flush()
        self.assertEqual(miltons_stapler['person'].no_refs(), milton.no_refs())
        self.assertEqual(_remove_refs(milton['possessions']), _remove_refs([miltons_stapler,]))

        # Milton has a manager
        tom = Person(name='Tom').flush()
        milton['manager_id'] = tom['id']
        milton.flush()
        self.assertEqual(milton['manager'], tom)

        # Tom takes milton's stapler
        miltons_stapler['person_id'] = tom['id']
        toms_stapler = miltons_stapler.flush()
        self.assertEqual(toms_stapler['person'].no_refs(), tom.no_refs())
        self.assertEqual(_remove_refs(tom['possessions']), _remove_refs([toms_stapler,]))

        # Peter is Tom's subordinate
        peter = Person(name='Peter', manager_id=tom['id']).flush()
        self.assertEqual(peter['manager'], tom)
        self.assertIn(peter.no_refs(), _remove_refs(tom['subordinates']))
        self.assertIn(milton.no_refs(), _remove_refs(tom['subordinates']))

        # Peter is also in sales
        PD(person_id=peter['id'], department_id=sales['id']).flush()
        self.assertIn(peter.no_refs(), _remove_refs(sales['persons']))
        self.assertIn(milton.no_refs(), _remove_refs(sales['persons']))

        # There are 3 people
        self.assertEqual(Person.count(), 3)
        if self.db.kind == 'postgresql':
            self.assertEqual(len(Person), 3)
            self.assertEqual(len(Person.get_where()), 3)

        # There are two salesmen
        self.assertEqual(len(list(PD.get_where(department_id=sales['id']))), 2)

        # Milton's car is shared
        peter['car_id'] = miltons_car['id']
        peter.flush()
        self.assertEqual(peter['car'], miltons_car)
        self.assertEqual(miltons_car['person'].no_refs(), milton.no_refs())
        self.assertEqual(peter['car'].no_refs(), miltons_car.no_refs())
        car_owners = Person.get_where(car_id=miltons_car['id'])
        self.assertEqual(_remove_refs(car_owners), _remove_refs([milton, peter]))

        # You can reuse a ResultsGenerator
        minions = tom['subordinates']
        self.assertEqual(_remove_refs(minions),
                _remove_refs([milton, peter]))
        limited_minions = minions.limit(1)
        self.assertEqual(_remove_refs(limited_minions),
                _remove_refs([milton,]))
        self.assertEqual(_remove_refs(limited_minions.order_by('id DESC')),
                _remove_refs([peter,]))
        # A modified ResultsGenerator creates a new query
        self.assertEqual(_remove_refs(minions.refine(Person['name']=='Milton')),
                _remove_refs([milton,]))
        self.assertEqual(_remove_refs(minions.refine(Person['name']=='Peter')),
                _remove_refs([peter,]))

        self.assertEqual(_remove_refs(Person.get_where(Person['id'].IsNot(None
            )).order_by('id ASC')),
                _remove_refs([milton, tom, peter]))
        self.assertEqual(_remove_refs(Person.get_where(Person['id']>0).order_by(
            'id ASC')),
                _remove_refs([milton, tom, peter]))


    def test_order_by2(self):
        """
        A result set can be refined using order by.  A reference can be refined
        using the same technique.
        """
        Person = self.db['person']
        Person['subordinates'] = Person['id'].many(Person['manager_id'])
        Person['manager'] = Person['id'] == Person['manager_id']
        bob = Person(name='Bob').flush()
        # Insert the employees with IDs that are reverse of the entrydate
        alice = Person(name='Alice', manager_id=bob['id'], id=3, other=2).flush()
        dave = Person(name='Dave', manager_id=bob['id'], id=2, other=3).flush()
        # Ordered by their ID by default
        result = Person.get_where()
        self.assertEqual(_remove_refs(Person.get_where()),
                _remove_refs([bob, dave, alice]))

        # Refine the results by ordering by other, which is the reverse of how
        # they were inserted
        self.assertEqual(_remove_refs(bob['subordinates'].order_by('other ASC')),
                _remove_refs([alice, dave]))
        self.assertEqual(_remove_refs(bob['subordinates']),
                _remove_refs([dave, alice]))

        steve = Person(name='Steve', manager_id=alice['id'], id=4).flush()
        self.assertEqual(_remove_refs(alice['subordinates']),
                _remove_refs([steve,]))

        all_subordinates = Person.get_where(Person['manager_id'].In((1,3)))
        self.assertEqual(list(all_subordinates), [dave, alice, steve])

        all_subordinates = Person.get_where(Person['manager_id'].In((1,3)))
        self.assertEqual(list(all_subordinates.refine(name='Alice')), [alice,])


    def test_offset_limit(self):
        """
        A result set can be refined using an offset and limit.
        """
        Person = self.db['person']
        bob = Person(name='Bob').flush()
        aly = Person(name='Aly').flush()
        tom = Person(name='Tom').flush()
        abe = Person(name='Abe').flush()
        gus = Person(name='Gus').flush()

        persons = Person.get_where()
        self.assertEqual(list(persons), [bob, aly, tom, abe, gus])
        self.assertEqual(list(persons), [bob, aly, tom, abe, gus])

        # Using limit and offset, but in such a way that it returns everything
        if self.db.kind == 'postgresql':
            self.assertEqual(list(persons.limit('ALL').offset(0)),
                    [bob, aly, tom, abe, gus])

        # Single refine
        limited = persons.limit(2)
        self.assertEqual(list(limited), [bob, aly])
        self.assertEqual(list(limited), [bob, aly])

        self.assertEqual(list(limited.offset(3)), [abe, gus])
        # Multiple refinings
        self.assertEqual(list(persons.limit(2).offset(2)), [tom, abe])


    def test_refine_comparisons(self):
        Person = self.db['person']
        Car = self.db['car']
        Person['subordinates'] = Person['id'].many(Person['manager_id'])
        bob = Person(name='Bob').flush()
        steves_car = Car().flush()
        steve = Person(name='Steve', car_id=steves_car['id'], manager_id=bob['id']).flush()
        aly = Person(name='Aly', manager_id=bob['id']).flush()
        frank = Person(name='Frank', manager_id=bob['id']).flush()

        self.assertEqual(list(bob['subordinates']),
                [steve, aly, frank])
        self.assertEqual(list(bob['subordinates'].order_by('id DESC')),
                [frank, aly, steve])
        self.assertEqual(list(bob['subordinates'].order_by('id DESC'
            ).limit(1)),
                [frank,])
        self.assertEqual(list(bob['subordinates'].order_by('id DESC'
            ).limit(1).offset(1)),
                [aly,])

        self.assertEqual(list(bob['subordinates'].refine(Person['car_id']>0)),
                [steve,])



    def test_onetoone_cache(self):
        """
        One-to-one relationships are cached.
        """
        Person = self.db['person']
        Person['manager'] = Person['manager_id'] == Person['id']
        bob = Person(name='Bob').flush()
        bill = Person(name='Bill').flush()
        bob['manager_id'] = bill['id']

        self.assertEqual(bob['manager'], bill)
        bob._table.get_one = error
        # Error fuction shouldn't be called, since manager is cached
        self.assertEqual(bob['manager'], bill)


    def test_results_cache(self):
        """
        A result will not be gotten again, since it's results were cached.
        """
        Person = self.db['person']
        Person['subordinates'] = Person['id'].many(Person['manager_id'])
        bob = Person(name='Bob').flush()
        bill = Person(name='Bill').flush()
        alice = Person(name='Alice').flush()
        steve = Person(name='Steve').flush()

        bill['manager_id'] = bob['id'];
        bill.flush()
        alice['manager_id'] = bob['id']
        alice.flush()
        steve['manager_id'] = bob['id']
        steve.flush()

        subordinates = bob['subordinates']
        for sub in subordinates:
            self.assertType(sub, Dict)
        # Error would be raised if subordinates isn't cached
        bob._table.get_where = error
        for sub in subordinates:
            self.assertType(sub, Dict)


    def test_offset(self):
        """
        Postgres allows offset without limit, but not Sqlite
        """
        Person = self.db['person']
        Person['subordinates'] = Person['id'].many(Person['manager_id'])
        bob = Person(name='Bob').flush()
        self.assertEqual(list(bob['subordinates'].offset(1)), [])


    def test_reference_order(self):
        """
        A reference definition cares about order.
        """
        Person = self.db['person']
        Person['manager'] = Person['manager_id'] == Person['id']
        bob = Person(name='Bob').flush()
        alice = Person(name='Alice', manager_id=bob['id']).flush()

        self.assertEqual(alice['manager'].no_refs(), bob.no_refs())
        Person['manager'] = Person['id'] == Person['manager_id']
        # Get alice again to clear cache
        alice = Person.get_one(id=2)
        self.assertEqual(alice['manager'], None)



class SqliteTestBase(object):

    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.db = DictDB(self.conn)
        self.curs = self.db.curs
        self.tearDown()
        self.curs.executescript('''
        CREATE TABLE person (
            id INTEGER PRIMARY KEY,
            name TEXT,
            other INTEGER,
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
            description JSONB
        );
        ''')
        self.conn.commit()
        self.db.refresh_tables()


    def tearDown(self):
        self.conn.rollback()
        self.curs.execute("""select 'drop table ' || name || ';' from
                sqlite_master where type = 'table';""")
        self.conn.commit()



class TestSqlite(SqliteTestBase, TestPostgresql):


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


    # Not supported for sqlite
    test_count = None
    test_delete = None
    test_json = None
    test_order_by2 = None
    test_refine_order_by = None
    test_second_cursor = None
    test_offset = None




if __name__ == '__main__':
    unittest.main()
