#! /usr/bin/env python
from pgpydict import (DictDB, PgPyTable, PgPyDict, UnexpectedRows, NoPrimaryKey,
    column_value_pairs)
from pprint import pprint
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

def _remove_refs(o):
    if type(o) == PgPyDict:
        return o.remove_refs()
    return [i.remove_refs() for i in o]


class Test(unittest.TestCase):

    def assertDictContains(self, d1, d2):
        assert set(d2.items()).issubset(set(d1.items())), '{} does not contain {}'.format(d1, d2)


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
        CREATE TABLE posession (
            id SERIAL PRIMARY KEY,
            person_id INTEGER,
            posession JSONB
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
        # PgPyDict row that matches that row's id
        self.assertEqual(list(Person.get_where(1)), [bob,])
        self.assertEqual(self.curs.rowcount, 1)

        # get_where with no parameters returns the entire table
        self.assertEqual(list(Person.get_where()), [bob, dave, alice])

        # A delete sql command can be executed on a PgPyDict
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


    def test_self_reference(self):
        """
        person
        id <----------\
        manager_id ---/
        """
        Person = self.db['person']
        Person['manager'] = Person['manager_id'] == Person['id']

        bob = Person(name='Bob')
        bob.flush()
        self.assertDictContains(bob, {'name':'Bob', 'id':1})
        aly = Person(name='Aly')
        aly.flush()

        bob['manager_id'] = aly['id']
        bob.flush()
        self.assertEqual(bob['manager_id'], aly['id'])
        self.assertEqual(bob['manager'].remove_refs(), aly.remove_refs())

        steve = Person(name='Steve')
        steve.flush()
        bob['manager_id'] = steve['id']
        self.assertEqual(bob['manager_id'], steve['id'])
        self.assertEqual(bob['manager'].remove_refs(), steve.remove_refs())

        bob['manager'] = aly
        bob.flush()
        self.assertEqual(bob['manager_id'], aly['id'])
        self.assertEqual(bob['manager'].remove_refs(), aly.remove_refs())


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
        toyota = Car(name='Toyota', person_id=bob['id']).flush()
        honda = Car(name='Honda', person_id=bob['id']).flush()
        ford = Car(name='Ford', person_id=bob['id']).flush()

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
        self.assertEqual(column_value_pairs({'id':10, 'person':'Dave'}),
                'id=%(id)s, person=%(person)s')
        self.assertEqual(column_value_pairs(('id', 'person')),
                'id=%(id)s, person=%(person)s')
        self.assertEqual(column_value_pairs({'id':(10,11,13), 'group':'foo'}, ' AND '),
                'group=%(group)s AND id IN %(id)s')
        self.assertEqual(column_value_pairs({'id':12, 'person':'Dave'}, prefix='old_'),
                'id=%(old_id)s, person=%(old_person)s')


    def test_second_cursor(self):
        """
        PgPyDict's cursor should not interfere with another cursor.
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

        # Using pgpydict's cursor will intefere
        persons = Person.get_where()
        self.assertEqual(next(persons), bob)
        self.db.curs.execute('SELECT * FROM person')
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
        self.assertEqual(len(list(NoPk.get_where())), 2)
        self.assertIn('ORDER BY foo desc', NoPk.curs.query.decode())

        NoPk.order_by = None
        self.assertEqual(len(list(NoPk.get_where(foo='bar'))), 1)
        self.assertNotIn('ORDER BY', NoPk.curs.query.decode())
        NoPk.order_by = 'foo desc'
        self.assertEqual(len(list(NoPk.get_where(foo='bar'))), 1)
        self.assertIn('ORDER BY foo desc', NoPk.curs.query.decode())


    def test_json(self):
        Posession = self.db['posession']
        p = Posession(posession={'foo':'bar', 'baz':1, True:False})
        p.flush()

        p['posession'] = {'foo':'baz'}
        p.flush()
        self.assertEqual(Posession.get_one()['posession'], {'foo':'baz'})


    def test_multiple_references(self):
        Person = self.db['person']
        Person['manager'] = Person['manager_id'] == Person['id']
        alice = Person(name='Alice').flush()

        dave = Person(name='Dave', manager_id=alice['id']).flush()
        self.assertEqual(dave['manager'].remove_refs(), alice.remove_refs())
        bob = Person(name='Bob', manager_id=alice['id']).flush()
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

        self.assertEqual(_remove_refs(dave['person_departments']),
                _remove_refs([hr_pd, sales_pd]))

        print(alice._curs.query)
        _remove_refs(alice['subordinates'])
        print(alice._curs.query)
        self.assertEqual(_remove_refs(alice['subordinates']),
                _remove_refs([dave, bob]))


if __name__ == '__main__':
    unittest.main()
