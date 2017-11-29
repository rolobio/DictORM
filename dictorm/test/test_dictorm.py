#! /usr/bin/env python
import dictorm
from psycopg2.extras import DictCursor
import os
import psycopg2
import sqlite3
import unittest


test_db_login = {
        'database':'postgres',
        'user':'postgres',
        'password':'dictorm',
        'host':'localhost',
        'port':'54321',
        }


if 'CI' in os.environ.keys():
    test_db_login['port'] = 5432


def _no_refs(o):
    if isinstance(o, dictorm.Dict):
        return o.no_refs()
    l = []
    for i in o:
        if isinstance(i, dictorm.Dict):
            l.append(i.no_refs())
        else:
            l.append(i)
    return l


def error(*a, **kw): raise Exception()


class ExtraTestMethods:

    @classmethod
    def assertDictContains(cls, d1, d2):
        if not set(d2.items()).issubset(set(d1.items())):
            raise TypeError('{0} does not contain {1}'.format(d1, d2))


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
            raise TypeError('{0} is not type {1}'.format(str(a), b0))


    def assertEqualNoRefs(self, a, b):
        return self.assertEqual(_no_refs(a), _no_refs(b))


    def assertInNoRefs(self, a, b):
        return self.assertIn(_no_refs(a), _no_refs(b))



class CommonTests(unittest.TestCase, ExtraTestMethods):
    """
    These tests will be run for all supported databases.
    """

    def setUp(self):
        self.conn = psycopg2.connect(**test_db_login)
        self.db = dictorm.DictDB(self.conn)
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
        if self.db.kind != 'sqlite3':
            self.assertEqual(list(Person.get_where(Person['id'].In([1,3]))),
                    [bob, alice])

        # Database row survives an object deletion
        del bob
        del alice
        self.conn.commit()
        self.assertEqual(Person.count(), 2)

        bob, alice = Person.get_where()
        bob.delete()
        alice.delete()
        self.assertEqual(Person.count(), 0)


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
        self.assertEqual(PD.count(), 0)


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

        dictorm.Dict(Person, {'name':'Bob'}).flush()
        dictorm.Dict(Person, name='Alice').flush()
        dictorm.Dict(Person, [('name','Steve'),]).flush()

        # A fake column will fail when going into the database
        p = Person(fake_column='foo')
        self.assertRaisesAny(dictorm.InvalidColumn, p.flush)
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
        self.assertEqualNoRefs(aly['person_departments'], [aly_pd_sales, aly_pd_hr])
        self.assertEqualNoRefs(bob['person_departments'], [bob_pd_sales,])


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
        self.assertEqual(list(bob['person_departments']), [bob_pd_sales, bob_pd_hr])


    def test_substratum_one(self):
        Person = self.db['person']
        Car = self.db['car']
        # Setup the initial references
        Person['manager'] = Person['manager_id'] == Person['id']
        Person['car'] = Person['car_id'] == Car['id']
        Person['manager_name'] = Person['manager'].substratum('name')
        Person['manager_car'] = Person['manager'].substratum('car')

        alice_car = Car(name='Prius').flush()
        alice = Person(name='Alice', car_id=alice_car['id']).flush()
        bob = Person(name='Bob', manager_id=alice['id']).flush()
        alice['manager_id'] = bob['id']
        alice.flush()

        self.assertEqualNoRefs(bob['manager_car'], alice_car)
        self.assertEqualNoRefs(bob['manager'], alice)

        # Overwriting a substratum doesn't break a flush
        self.assertEqual(bob['manager_name'], 'Alice')
        bob['manager_name'] = 'foo'
        bob.flush()
        self.assertEqual(bob['manager_name'], 'foo')


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

        self.assertEqual(list(bob.references().keys()), ['cars',])


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

        self.assertEqualNoRefs(will.get('car'), stratus)
        self.assertEqualNoRefs(will['car'], stratus)
        self.assertEqualNoRefs(stratus['person'], will)
        self.assertEqual(list(will.references().keys()), ['car',])


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
        self.assertEqualNoRefs(bob['manager'], bob)
        self.assertEqual(list(bob.references().keys()), ['manager',])


    def test_errors(self):
        """
        A table with no primary key(s) can be gotten, but not updated.
        """
        Person = self.db['person']

        bob = Person(name='Bob').flush()
        aly = Person(name='Aly').flush()

        self.assertRaises(dictorm.NoPrimaryKey, Person.get_where, 1,2)

        self.assertRaises(KeyError, bob.__getitem__, 'foo')

        self.assertRaises(dictorm.UnexpectedRows, Person.get_one)

        bob['not_real_column'] = 1
        self.assertRaisesAny(dictorm.InvalidColumn, bob.flush)
        self.conn.rollback()

        NoPk = self.db['no_pk']
        foo = NoPk(foo='bar')
        foo.flush()
        self.conn.commit()
        self.assertEqual(foo, {'foo':'bar'})
        self.assertEqual(list(NoPk.get_where()), [{'foo':'bar'},])
        foo['foo'] = 'baz'
        self.assertRaises(dictorm.NoPrimaryKey, foo.flush)
        self.assertRaises(dictorm.NoPrimaryKey, NoPk.get_where, 1)


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
        self.assertEqual(NoPk.count(), 2)
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
        self.assertEqualNoRefs(dave['manager'], alice)
        bob = Person(name='Bob', manager_id=alice['id']).flush()
        self.assertNotEqual(bob['manager'], None)
        self.assertEqualNoRefs(bob['manager'], alice)

        # New reference, no flush required
        Person['subordinates'] = Person['id'].many(Person['manager_id'])
        self.assertEqualNoRefs(alice['subordinates'],
                [dave, bob])

        # Changes survive a commit/flush
        self.conn.commit()
        bob.flush()
        alice.flush()
        dave.flush()
        self.assertEqualNoRefs(alice['subordinates'],
                [dave, bob])
        self.assertEqualNoRefs(dave['manager'], alice)
        self.assertEqualNoRefs(bob['manager'], alice)

        PD, Department = self.db['person_department'], self.db['department']
        PD['department'] = PD['department_id'] == Department['id']
        Person['person_departments'] = Person['id'].many(PD['person_id'])

        hr = Department(name='HR').flush()
        sales = Department(name='Sales').flush()
        hr_pd = PD(department_id=hr['id'], person_id=dave['id']).flush()
        sales_pd = PD(department_id=sales['id'], person_id=dave['id']).flush()

        # All references are available on demand
        self.assertEqualNoRefs(dave['person_departments'],
                [hr_pd, sales_pd])
        self.assertEqualNoRefs(alice['subordinates'],
                [dave, bob])
        self.assertEqualNoRefs(dave['manager'], alice)
        self.assertEqualNoRefs(bob['manager'], alice)

        # You can iterate through subordinates using a for loop
        for sub in alice['subordinates']:
            for pd in sub['person_departments']:
                pd.delete()
            sub.delete()


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
        Person['managers_manager'] = Person['manager'].substratum('manager')
        self.assertEqual(alice['manager'], None)
        # An empty substratum doesn't error
        self.assertEqual(alice['managers_manager'], None)


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
        self.assertEqualNoRefs(alice['manager'], steve)


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
        self.assertEqualNoRefs(milton['car'], miltons_car)
        milton.flush()
        miltons_car.flush()
        self.assertEqual(milton['car'], miltons_car)

        # Milton is in Sales
        milton_sales = PD(person_id=milton['id'], department_id=sales['id']).flush()
        self.assertEqualNoRefs(milton_sales, PD.get_one())
        self.assertEqualNoRefs(milton_sales['person'], milton)
        self.assertEqualNoRefs(milton_sales['department'], sales)
        self.assertEqualNoRefs(milton['departments'], [sales,])
        self.assertEqualNoRefs(sales['persons'], [milton,])

        # Milton has a stapler
        miltons_stapler = Possession(person_id=milton['id'],
                description={'kind':'stapler', 'brand':'Swingline', 'color':'Red'}
                ).flush()
        self.assertEqualNoRefs(miltons_stapler['person'], milton)
        self.assertEqualNoRefs(milton['possessions'], [miltons_stapler,])

        # Milton has a manager
        tom = Person(name='Tom').flush()
        milton['manager_id'] = tom['id']
        milton.flush()
        self.assertEqual(milton['manager'], tom)

        # Tom takes milton's stapler
        miltons_stapler['person_id'] = tom['id']
        toms_stapler = miltons_stapler.flush()
        self.assertEqualNoRefs(toms_stapler['person'], tom)
        self.assertEqualNoRefs(tom['possessions'], [toms_stapler,])

        # Peter is Tom's subordinate
        peter = Person(name='Peter', manager_id=tom['id']).flush()
        self.assertEqual(peter['manager'], tom)
        self.assertInNoRefs(peter, tom['subordinates'])
        self.assertInNoRefs(milton, tom['subordinates'])

        # Peter is also in sales
        PD(person_id=peter['id'], department_id=sales['id']).flush()
        self.assertInNoRefs(peter, sales['persons'])
        self.assertInNoRefs(milton, sales['persons'])

        # There are 3 people
        self.assertEqual(Person.count(), 3)
        if self.db.kind == 'postgresql':
            self.assertEqual(Person.count(), 3)
            self.assertEqual(len(Person.get_where()), 3)

        # There are two salesmen
        self.assertEqual(len(list(PD.get_where(department_id=sales['id']))), 2)

        # Milton's car is shared
        peter['car_id'] = miltons_car['id']
        peter.flush()
        self.assertEqual(peter['car'], miltons_car)
        self.assertEqualNoRefs(miltons_car['person'], milton)
        self.assertEqualNoRefs(peter['car'], miltons_car)
        car_owners = Person.get_where(car_id=miltons_car['id'])
        self.assertEqualNoRefs(car_owners, [milton, peter])

        # You can reuse a ResultsGenerator
        minions = tom['subordinates']
        self.assertEqualNoRefs(minions, [milton, peter])
        limited_minions = minions.limit(1)
        self.assertEqualNoRefs(limited_minions, [milton,])
        self.assertEqualNoRefs(limited_minions.order_by('id DESC'), [peter,])
        # A modified ResultsGenerator creates a new query
        self.assertEqualNoRefs(minions.refine(Person['name']=='Milton'),
                [milton,])
        self.assertEqualNoRefs(minions.refine(Person['name']=='Peter'), [peter,])

        self.assertEqualNoRefs(Person.get_where(Person['id'].IsNot(None
            )).order_by('id ASC'),
                [milton, tom, peter])
        self.assertEqualNoRefs(Person.get_where(Person['id']>0).order_by(
            'id ASC'),
                [milton, tom, peter])


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


    @unittest.expectedFailure
    def test_onetoone_cache(self):
        """
        One-to-one relationships are cached.

        TODO This fails because the cached object's row was changed
        """
        Person = self.db['person']
        Person['manager'] = Person['manager_id'] == Person['id']
        bob = Person(name='Bob').flush()
        bill = Person(name='Bill').flush()
        bob['manager_id'] = bill['id']

        self.assertEqual(bob['manager'], bill)
        old_get_one = bob._table.get_one
        bob._table.get_one = error
        # Error fuction shouldn't be called, since manager is cached
        self.assertEqual(bob['manager'], bill)

        Car = self.db['car']
        Person['car'] = Person['car_id'] == Car['id']
        Person['manager_car'] = Person['manager'].substratum('car')

        bob._table.get_one = old_get_one
        self.assertEqual(bob['manager_car'], None)
        bill_car = Car(name='Prius').flush()
        bill['car_id'] = bill_car['id']

        self.assertEqualNoRefs(bob['manager'], bill)
        self.assertEqualNoRefs(bob['manager_car'], bill_car)


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
            self.assertType(sub, dictorm.Dict)
        # Error would be raised if subordinates isn't cached
        bob._table.get_where = error
        for sub in subordinates:
            self.assertType(sub, dictorm.Dict)


    def test_reference_order(self):
        """
        A reference definition cares about order.
        """
        Person = self.db['person']
        Person['manager'] = Person['manager_id'] == Person['id']
        bob = Person(name='Bob').flush()
        alice = Person(name='Alice', manager_id=bob['id']).flush()

        self.assertEqualNoRefs(alice['manager'], bob)
        Person['manager'] = Person['id'] == Person['manager_id']
        # Get alice again to clear cache
        alice = Person.get_one(id=2)
        self.assertEqual(alice['manager'], None)


    def test_columns(self):
        """
        Table.columns is a method that gets a list of a table's columns
        """
        Person = self.db['person']
        self.assertEqual(Person.columns,
                ['id', 'name', 'other', 'manager_id', 'car_id'])


    def test_column_info(self):
        """
        Table.columns is a method that gets a list of a table's columns
        """
        Person = self.db['person']
        test_info = [
                {'column_name':'id', 'data_type':'integer'},
                {'column_name':'name', 'data_type':'text'},
                {'column_name':'other', 'data_type':'integer'},
                {'column_name':'manager_id', 'data_type':'integer'},
                {'column_name':'car_id', 'data_type':'integer'},
                ]
        self.assertEqual(len(test_info), len(Person.columns_info))
        for i,j in zip(test_info, Person.columns_info):
            self.assertDictContains(j, i)


    def test_like(self):
        Person = self.db['person']
        bob = Person(name='Bob').flush()
        self.assertEqualNoRefs(Person.get_where(Person['name'].Like('Bob')),
                [bob,])
        self.assertEqualNoRefs(Person.get_where(Person['name'].Like('%Bo%')),
                [bob,])


    def test_table_cls(self):
        class NewTable(dictorm.Table): pass
        self.db.table_factory = lambda: NewTable
        self.db.refresh_tables()
        self.assertIsInstance(self.db['person'], NewTable)


    def test_indexing(self):
        Person = self.db['person']
        result = Person.get_where()
        self.assertRaises(IndexError, result.__getitem__, 0)

        bob = Person(name='Bob').flush()
        alice = Person(name='Alice').flush()
        steve = Person(name='Steve').flush()

        result = Person.get_where()
        self.assertEqual(result[0], bob)
        self.assertEqual(result[0], bob)
        self.assertEqual(result[2], steve)
        self.assertEqual(result[-1], steve)
        self.assertEqual(result[-1], steve)
        self.assertEqual(result[1:], [alice, steve])


    def test_concurrent(self):
        """
        A ResultsGenerator is on it's own transaction.  Changing a row's values
        will not be reflected in the existing Results.
        """
        Person = self.db['person']
        bob = Person(name='Bob').flush()
        alice = Person(name='Alice').flush()

        results = Person.get_where()
        self.assertEqualNoRefs(results[0], bob)

        alice['name'] = 'Amy'
        alice.flush()

        # Gotten result contains the old value
        self.assertEqual(next(results)['name'], 'Alice')
        # Alice was changed
        self.assertEqual(alice['name'], 'Amy')
        self.conn.commit()
        self.assertEqual(alice['name'], 'Amy')


    def test_nocache(self):
        """
        A ResultsGenerator can be told not to cache results.
        """
        Person = self.db['person']
        bob = Person(name='Bob').flush()
        alice = Person(name='Alice').flush()

        results = Person.get_where().nocache()
        # Cache all results, if caching was enabled
        self.assertEqual(next(results), bob)
        self.assertEqual(next(results), alice)
        self.assertRaises(StopIteration, next, results)

        # Cache is empty
        self.assertEqual(results.cache, [])

        # Cannot iterate through results more than once
        self.assertRaises(dictorm.NoCache, results.__getitem__, 0)


    def test_aggregate(self):
        """
        A chain of many substratums creates an aggregate of the results.
        """
        Person, Department = self.db['person'], self.db['department']
        PD = self.db['person_department']
        PD.pks = ['person_id', 'department_id']

        # Relations
        PD['department'] = PD['department_id'] == Department['id']
        PD['person'] = PD['person_id'] == Department['id']
        Person['person_departments'] = Person['id'].many(PD['person_id'])
        Person['departments'] = Person['person_departments'].substratum(
                'department')
        Person['manager'] = Person['manager_id'] == Person['id']
        Person['subordinates'] = Person['id'].many(Person['manager_id'])
        Person['subordinates_departments'] = Person['subordinates'].aggregate(
                'departments')

        # People, with manager
        bob = Person(name='Bob').flush()
        alice = Person(name='Alice', manager_id=bob['id']).flush()
        steve = Person(name='Steve', manager_id=bob['id']).flush()

        # Departments
        sales = Department(name='Sales').flush()
        hr = Department(name='HR').flush()
        it = Department(name='IT').flush()

        # Person_Departments
        steve_sales = PD(person_id=steve['id'], department_id=sales['id']).flush()
        steve_hr = PD(person_id=steve['id'], department_id=hr['id']).flush()
        alice_it = PD(person_id=alice['id'], department_id=it['id']).flush()

        self.assertEqualNoRefs(alice['manager'], bob)
        self.assertEqualNoRefs(steve['manager'], bob)
        self.assertEqualNoRefs(bob['subordinates'], [alice, steve])
        self.assertEqualNoRefs(bob['subordinates_departments'], [it, sales, hr])


    def test_value_types(self):
        """
        When a row is updated, the flush should return values of the correct
        type.
        """
        Person = self.db['person']
        bob = Person(name='Bob').flush()
        self.assertEqual(bob['id'], 1)

        # Update "id" using a string
        bob.update({'id':'1', 'name':'Steve'})
        steve = bob.flush()
        self.assertEqual(steve['id'], 1) # ID should be an integer
        self.assertEqual(steve['name'], 'Steve')


    def test_injection(self):
        """
        A column name can't be used for injection
        """
        Person = self.db['person']
        bob = Person(name='Bob').flush()
        self.assertEqual(bob['id'], 1)
        self.conn.commit()

        # An entry can still be flushed even if a column is missing
        del bob['manager_id']
        self.assertNotIn('manager_id', bob)
        bob.flush()
        self.assertIn('manager_id', bob)

        # An invalid column name raises an error
        bob[' "; DELETE FROM person;'] = 'Bob'
        self.assertRaises(dictorm.InvalidColumn, bob.flush)


class TestPostgresql(CommonTests):


    def test_columns_property(self):
        """
        Table.columns and Table.columns_info are properties, and should only get
        their values once.

        Not supported under Sqlite3
        """
        Person = self.db['person']
        original_execute = self.curs.execute

        col_vals = ['id', 'name', 'other', 'manager_id', 'car_id']
        self.assertEqual(Person.columns, col_vals)

        try:
            Person.curs.execute = error
            # Error shouldn't be raised
            self.assertEqual(Person.columns, col_vals)
        finally:
            Person.curs.execute = original_execute


    def test_count(self):
        """
        Simple reference counting is supported.
        """
        Person = self.db['person']
        Person['subordinates'] = Person['id'].many(Person['manager_id'])
        alice = Person(name='Alice').flush()
        dave = Person(name='Dave', manager_id=alice['id']).flush()
        bob = Person(name='Bob', manager_id=alice['id']).flush()
        self.assertIsInstance(alice['subordinates'], dictorm.ResultsGenerator)
        self.assertNotIn(alice._curs.query.decode(), 'SELECT *')
        # get len() without running a larger query
        self.assertEqual(len(alice['subordinates']), 2)
        # you can still get the same old results even after running a len()
        self.assertEqualNoRefs(alice['subordinates'], [dave, bob])
        # the generator can be converted to a list
        self.assertEqualNoRefs(list(alice['subordinates']), [dave, bob])

        subs = alice['subordinates']
        self.assertEqual(len(subs), 2)
        self.assertEqualNoRefs(subs, [dave, bob])


    def test_ilike(self):
        Person = self.db['person']
        alice = Person(name='Alice').flush()
        self.assertEqualNoRefs(Person.get_where(Person['name'].Ilike('ali%')),
                [alice,])


    def test_json(self):
        Possession = self.db['possession']
        p = Possession(description={'foo':'bar', 'baz':1}).flush()
        self.assertEqual(Possession.get_one()['description'], {'foo':'bar', 'baz':1})

        # Testing an update of a json
        p['description'] = {'foo':'baz'}
        p.flush()
        self.assertEqual(Possession.get_one()['description'], {'foo':'baz'})

        # Non-json row doesn't call json_dicts
        original = dictorm.set_json_dicts(error)
        self.assertRaises(Exception, p.flush)
        try:
            Person = self.db['person']
            bob = Person(name='Bob')
            # Shouldn't raise Exception from "error" function
            bob.flush()
        finally:
            dictorm.set_json_dicts(original)


    def test_offset(self):
        """
        Postgres allows offset without limit, but not Sqlite
        """
        Person = self.db['person']
        Person['subordinates'] = Person['id'].many(Person['manager_id'])
        bob = Person(name='Bob').flush()
        self.assertEqual(list(bob['subordinates'].offset(1)), [])


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
        self.assertEqualNoRefs(Person.get_where(),
                [bob, dave, alice])

        # Refine the results by ordering by other, which is the reverse of how
        # they were inserted
        self.assertEqualNoRefs(bob['subordinates'].order_by('other ASC'),
                [alice, dave])
        self.assertEqualNoRefs(bob['subordinates'],
                [dave, alice])

        steve = Person(name='Steve', manager_id=alice['id'], id=4).flush()
        self.assertEqualNoRefs(alice['subordinates'], [steve,])

        all_subordinates = Person.get_where(Person['manager_id'].In((1,3)))
        self.assertEqual(list(all_subordinates), [dave, alice, steve])

        all_subordinates = Person.get_where(Person['manager_id'].In((1,3)))
        self.assertEqual(list(all_subordinates.refine(name='Alice')), [alice,])


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



class SqliteTestBase(object):

    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.db = dictorm.DictDB(self.conn)
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



class TestSqlite(SqliteTestBase, CommonTests):


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
        self.assertEqual(Person.count(), 2)

        bob, alice = Person.get_where()
        bob.delete()
        alice.delete()
        self.assertEqual(Person.count(), 0)


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
        self.assertEqual(NoPk.count(), 2)
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


    def test_column_info(self):
        """
        Table.columns is a method that gets a list of a table's columns
        """
        Person = self.db['person']
        test_info = [
                {'name':'id', 'type':'INTEGER'},
                {'name':'name', 'type':'TEXT'},
                {'name':'other', 'type':'INTEGER'},
                {'name':'manager_id', 'type':'INTEGER'},
                {'name':'car_id', 'type':'INTEGER'},
                ]
        self.assertEqual(len(test_info), len(Person.columns_info))
        for i,j in zip(test_info, [dict(i) for i in Person.columns_info]):
            self.assertDictContains(j, i)



if __name__ == '__main__':
    unittest.main()
