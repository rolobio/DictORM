import unittest
from dictorm.pg import Select, Insert, Update, Delete, Or, Xor, And, Column, set_sort_keys
from dictorm.pg import Join, LeftJoin, RightJoin, InnerJoin, FullOuterJoin, FullJoin
from dictorm import Table

class PersonTable(object):
    '''fake DictORM Table for testing'''

    name = 'person'

    def __init__(self):
        self.fks = {}
        self.refs = {}


    def __getitem__(self, key):
        if key in self.refs:
            return self.refs[key]
        return Column(self, key)

    __setitem__ = Table.__setitem__



class CarTable(PersonTable):
    '''fake DictORM Table for testing'''

    name = 'car'



class DeptTable(PersonTable):
    '''fake DictORM Table for testing'''

    name = 'dept'


Person = PersonTable()
Car = CarTable()
Dept = DeptTable()
set_sort_keys(True)


class TestSelect(unittest.TestCase):


    def test_basic(self):
        q = Select('some_table')
        self.assertEqual(str(q), 'SELECT * FROM "some_table"')
        q = Select('some_table', And())
        self.assertEqual(str(q), 'SELECT * FROM "some_table"')
        q = Select('some_table', Person['name'] == 'Bob')
        self.assertEqual(str(q), 'SELECT * FROM "some_table" WHERE "person"."name"=%s')
        q = Select('some_table', 'Bob' == Person['name'])
        self.assertEqual(str(q), 'SELECT * FROM "some_table" WHERE "person"."name"=%s')
        q = Select('some_table', Person['name'] > 'Bob')
        self.assertEqual(str(q), 'SELECT * FROM "some_table" WHERE "person"."name">%s')
        q = Select('some_table', Person['name'] >= 'Bob')
        self.assertEqual(str(q), 'SELECT * FROM "some_table" WHERE "person"."name">=%s')
        q = Select('some_table', Person['name'] < 'Bob')
        self.assertEqual(str(q), 'SELECT * FROM "some_table" WHERE "person"."name"<%s')
        q = Select('some_table', Person['name'] <= 'Bob')
        self.assertEqual(str(q), 'SELECT * FROM "some_table" WHERE "person"."name"<=%s')
        q = Select('some_table', Person['name'] != 'Bob')
        self.assertEqual(str(q), 'SELECT * FROM "some_table" WHERE "person"."name"!=%s')


    def test_returning(self):
        q = Select('some_table', Person['name'] == 'Bob', returning='*')
        self.assertEqual(str(q), 'SELECT * FROM "some_table" WHERE "person"."name"=%s RETURNING *')
        self.assertEqual(q.build(), (
            'SELECT * FROM "some_table" WHERE "person"."name"=%s RETURNING *',
            ['Bob',]))


        q = Select('some_table', 'Bob' == Person['name'], returning='id')
        self.assertEqual(str(q),
                'SELECT * FROM "some_table" WHERE "person"."name"=%s RETURNING "id"')
        self.assertEqual(q.build(), (
            'SELECT * FROM "some_table" WHERE "person"."name"=%s RETURNING "id"',
            ['Bob',]
            ))


    def test_logical(self):
        bob_name = Person['name'] == 'Bob'
        bob_car = Person['car_id'] == 2
        q = Select('some_table', bob_name.Or(bob_car))
        self.assertEqual(str(q), 'SELECT * FROM "some_table" WHERE "person"."name"=%s OR "person"."car_id"=%s')
        q = Select('some_table', bob_name.And(bob_car))
        self.assertEqual(str(q), 'SELECT * FROM "some_table" WHERE "person"."name"=%s AND "person"."car_id"=%s')
        q = Select('some_table', bob_name.Xor(bob_car))
        self.assertEqual(str(q), 'SELECT * FROM "some_table" WHERE "person"."name"=%s XOR "person"."car_id"=%s')


    def test_logical_groups(self):
        q = Select('some_table', Xor(
            And(Person['name'] == 'Bob', Person['car_id'] == 2),
            Person['name'] == 'Alice'
            ))

        self.assertEqual(str(q),
                'SELECT * FROM "some_table" WHERE ("person"."name"=%s AND "person"."car_id"=%s) XOR "person"."name"=%s')

        q = Select('some_table', Or(
            And(Person['name'] >= 'Bob', Person['car_id'] == 2.3),
            And(Person['name'] <= 'Alice', Person['car_id'] != 3)
            ))
        self.assertEqual(str(q),
                'SELECT * FROM "some_table" WHERE ("person"."name">=%s AND "person"."car_id"=%s) OR ("person"."name"<=%s AND "person"."car_id"!=%s)')


    def test_build(self):
        """
        building the query results in a tuple of the sql and a list of values
        that need to be interpolated into the sql by Psycopg2.
        """
        q = Select('other_table', Person['name'] == 'Steve')
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name"=%s',
                    ['Steve',]
                    )
                )

        q = Select('other_table', And(Person['name'] == 'Steve', Person['car_id'] == 12))
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name"=%s AND "person"."car_id"=%s',
                    ['Steve', 12]
                    )
                )

        q = Select('other_table', Or(
            And(Person['name'] == 'Steve', Person['car_id'] == 12),
            And(Person['name'] == 'Bob', Person['car_id'] == 1)
            )
            )
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE ("person"."name"=%s AND "person"."car_id"=%s) OR ("person"."name"=%s AND "person"."car_id"=%s)',
                    ['Steve', 12, 'Bob', 1]
                    )
                )

        q = Select('other_table', Person['name'].Is('Bob'))
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name" IS %s',
                ['Bob',])
                )
        q = Select('other_table', Person['name'].IsNot('Bob'))
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name" IS NOT %s',
                ['Bob',])
                )
        q = Select('other_table', Person['name'].IsNull())
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name" IS NULL',
                [])
                )
        q = Select('other_table', And(
            Person['name'].IsNull(),
            Person['foo'] == 'bar',
            Person['baz'].Is('bake'),
            Person['whatever'].IsDistinct('foo'),
            Person['whatever'].IsNotDistinct('bar'),
            ))
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name" IS NULL AND "person"."foo"=%s AND "person"."baz" IS %s AND "person"."whatever" IS DISTINCT FROM %s AND "person"."whatever" IS NOT DISTINCT FROM %s',
                ['bar', 'bake', 'foo', 'bar'])
                )
        q = Select('other_table', Person['name'].IsNotNull())
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name" IS NOT NULL',
                [])
                )


    def test_order_by(self):
        q = Select('other_table', Person['name'] == 'Steve').order_by('id ASC')
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name"=%s ORDER BY id ASC',
                    ['Steve',]
                    )
                )


    def test_limit(self):
        q = Select('other_table', Person['name'] == 'Steve').order_by('id ASC'
                ).limit(12)
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name"=%s ORDER BY id ASC LIMIT 12',
                    ['Steve',]
                    )
                )


    def test_offset(self):
        q = Select('other_table', Person['name'] == 'Steve').order_by('id ASC'
                ).offset(8)
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name"=%s ORDER BY id ASC OFFSET 8',
                    ['Steve',]
                    )
                )
        q = Select('other_table', Person['name'] == 'Steve').order_by('id ASC'
                ).offset(8).limit(12)
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name"=%s ORDER BY id ASC LIMIT 12 OFFSET 8',
                    ['Steve',]
                    )
                )


    def test_like(self):
        q = Select('other_table', Person['name'].Like('Steve'))
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name" LIKE %s',
                    ['Steve',]
                    )
                )
        q = Select('other_table', Person['name'].Ilike('Steve'))
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name" ILIKE %s',
                    ['Steve',]
                    )
                )
        q = Select('other_table', Person['name'].Like('%Steve%'))
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name" LIKE %s',
                    ['%Steve%',]
                    )
                )
        q = Select('other_table', Person['name'].Ilike('%Steve%'))
        self.assertEqual(q.build(),
                ('SELECT * FROM "other_table" WHERE "person"."name" ILIKE %s',
                    ['%Steve%',]
                    )
                )


    def test_select_tuple(self):
        q = Select('cool_table', Person['id'].In((1,2)))
        self.assertEqual(q.build(),
                ('SELECT * FROM "cool_table" WHERE "person"."id" IN %s',
                    [(1,2),]
                    )
                )

        q = Select('cool_table', Person['id'].In((1,2))).order_by('id DESC')
        self.assertEqual(q.build(),
                ('SELECT * FROM "cool_table" WHERE "person"."id" IN %s ORDER BY id DESC',
                    [(1,2),]
                    )
                )




class TestJoin(unittest.TestCase):

    def test_build(self):
        q = Join(Person['car_id'] == Car['id'])
        self.assertEqual(q.build(),
                ('SELECT "person".* FROM "person" JOIN "car" ON "car"."id"="person"."car_id"',
                    [])
                )
        # First column is the table being joined to
        q = Join(Car['id'] == Person['car_id'])
        self.assertEqual(q.build(),
                ('SELECT "car".* FROM "car" JOIN "person" ON "person"."car_id"="car"."id"',
                    [])
                )
        q = Join(Person['car_id'] == Car['id'], Dept['person_id'] == Person['id'])
        self.assertEqual(q.build(),
                ('SELECT "person".* FROM "person" JOIN "car" ON "car"."id"="person"."car_id" JOIN "dept" ON "dept"."person_id"="person"."id"',
                    [])
                )


    def test_multi_join(self):
        q = Join(Person['car_id'] == Car['id']).LeftJoin(Dept['person_id'] == Person['id'])
        self.assertEqual(q.build(),
                ('SELECT "person".* FROM "person" JOIN "car" ON "car"."id"="person"."car_id" LEFT JOIN "dept" ON "dept"."person_id"="person"."id"',
                    [])
                )
        q = Join(Person['dept_id'] == Dept['id']).LeftJoin(Car['dept_id'] == Dept['id'])
        self.assertEqual(q.build(),
                ('SELECT "person".* FROM "person" JOIN "dept" ON "dept"."id"="person"."dept_id" LEFT JOIN "car" ON "car"."dept_id"="dept"."id"',
                    [])
                )
        q = Join(Person['dept_id'] == Dept['id']).LeftJoin(Dept['id'] == Car['dept_id'])
        self.assertEqual(q.build(),
                ('SELECT "person".* FROM "person" JOIN "dept" ON "dept"."id"="person"."dept_id" LEFT JOIN "dept" ON "dept"."id"="car"."dept_id"',
                    [])
                )



class TestInsert(unittest.TestCase):


    def test_basic(self):
        q = Insert('some_table', name='Bob')
        self.assertEqual(str(q), 'INSERT INTO "some_table" ("name") VALUES (%s)')
        q = Insert('some_table', name='Bob', id=3)
        self.assertEqual(str(q), 'INSERT INTO "some_table" ("id", "name") VALUES (%s, %s)')
        q = Insert('some_table')
        self.assertEqual(str(q), 'INSERT INTO "some_table" DEFAULT VALUES')


    def test_build(self):
        q = Insert('some_table', name='Bob')
        self.assertEqual(q.build(),
                ('INSERT INTO "some_table" ("name") VALUES (%s)',
                    ['Bob',])
                )
        q = Insert('some_table', name='Bob', id=3)
        self.assertEqual(q.build(),
                ('INSERT INTO "some_table" ("id", "name") VALUES (%s, %s)',
                    [3, 'Bob'])
                )
        q = Insert('some_table', name='Bob', id=3, car_id=2)
        self.assertEqual(q.build(),
                ('INSERT INTO "some_table" ("car_id", "id", "name") VALUES (%s, %s, %s)',
                    [2, 3, 'Bob'])
                )


    def test_returning(self):
        q = Insert('other_table', name='Bob').returning('id')
        self.assertEqual(q.build(),
                ('INSERT INTO "other_table" ("name") VALUES (%s) RETURNING "id"',
                    ['Bob',])
                )



class TestUpdate(unittest.TestCase):


    def test_build(self):
        q = Update('some_table', name='Bob')
        self.assertEqual(q.build(), (
            'UPDATE "some_table" SET "name"=%s',
            ['Bob',]))

        q = Update('some_table', name='Bob', car_id=2)
        self.assertEqual(q.build(), (
            'UPDATE "some_table" SET "car_id"=%s, "name"=%s',
            [2, 'Bob']))

        q = Update('some_table', name='Bob', car_id=2).returning('*')
        self.assertEqual(q.build(), (
            'UPDATE "some_table" SET "car_id"=%s, "name"=%s RETURNING *',
            [2, 'Bob']))

        q = Update('some_table', name='Bob', car_id=2).where(
                Person['id']==3).returning('id')
        self.assertEqual(q.build(), (
            'UPDATE "some_table" SET "car_id"=%s, "name"=%s WHERE "person"."id"=%s RETURNING "id"',
            [2, 'Bob', 3]))

        q = Update('some_table', name='Bob', car_id=2).where(
                And(Person['id']==3, Person['car_id']==4)).returning('*')
        self.assertEqual(q.build(), (
            'UPDATE "some_table" SET "car_id"=%s, "name"=%s WHERE "person"."id"=%s AND "person"."car_id"=%s RETURNING *',
            [2, 'Bob', 3, 4]))

        wheres = And()
        wheres += Person['id']==3
        wheres += Person['car_id']==4
        q = Update('some_table', name='Bob', car_id=2).where(wheres).returning('*')
        self.assertEqual(q.build(), (
            'UPDATE "some_table" SET "car_id"=%s, "name"=%s WHERE "person"."id"=%s AND "person"."car_id"=%s RETURNING *',
            [2, 'Bob', 3, 4]))



class TestDelete(unittest.TestCase):

    def test_build(self):
        q = Delete('some_table').where(Person['name']=='Bob')
        self.assertEqual(q.build(), (
            'DELETE FROM "some_table" WHERE "person"."name"=%s',
            ['Bob',]))

        q = Delete('some_table').where(Person['name']>='Bob')
        self.assertEqual(q.build(), (
            'DELETE FROM "some_table" WHERE "person"."name">=%s',
            ['Bob',]))



if __name__ == '__main__':
    unittest.main()
