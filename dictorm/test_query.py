import unittest
from .pg import Select, Insert, Update, Delete, Or, Xor, And, Column

class PersonTable(object):
    '''fake DictORM Table for testing'''

    name = 'person'

    def __getitem__(self, key):
        return Column(self, key)


Person = PersonTable()


class TestSelect(unittest.TestCase):


    def test_basic(self):
        q = Select('some_table')
        self.assertEqual(str(q), "SELECT * FROM some_table")
        q = Select('some_table', And())
        self.assertEqual(str(q), "SELECT * FROM some_table")
        q = Select('some_table', Person['name'] == 'Bob')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name=%s")
        q = Select('some_table', 'Bob' == Person['name'])
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name=%s")
        q = Select('some_table', Person['name'] > 'Bob')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name>%s")
        q = Select('some_table', Person['name'] >= 'Bob')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name>=%s")
        q = Select('some_table', Person['name'] < 'Bob')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name<%s")
        q = Select('some_table', Person['name'] <= 'Bob')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name<=%s")
        q = Select('some_table', Person['name'] != 'Bob')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name!=%s")


    def test_returning(self):
        q = Select('some_table', Person['name'] == 'Bob', returning='*')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name=%s RETURNING *")
        self.assertEqual(q.build(), (
            "SELECT * FROM some_table WHERE name=%s RETURNING *",
            ['Bob',]))


        q = Select('some_table', 'Bob' == Person['name'], returning='id')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name=%s RETURNING id")
        self.assertEqual(q.build(), (
            "SELECT * FROM some_table WHERE name=%s RETURNING id",
            ['Bob',]
            ))


    def test_logical(self):
        bob_name = Person['name'] == 'Bob'
        bob_car = Person['car_id'] == 2
        q = Select('some_table', bob_name.Or(bob_car))
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name=%s OR car_id=%s")
        q = Select('some_table', bob_name.And(bob_car))
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name=%s AND car_id=%s")
        q = Select('some_table', bob_name.Xor(bob_car))
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name=%s XOR car_id=%s")


    def test_logical_groups(self):
        q = Select('some_table', Xor(
            And(Person['name'] == 'Bob', Person['car_id'] == 2),
            Person['name'] == 'Alice'
            ))

        self.assertEqual(str(q),
                "SELECT * FROM some_table WHERE (name=%s AND car_id=%s) XOR name=%s")

        q = Select('some_table', Or(
            And(Person['name'] >= 'Bob', Person['car_id'] == 2.3),
            And(Person['name'] <= 'Alice', Person['car_id'] != 3)
            ))
        self.assertEqual(str(q),
                "SELECT * FROM some_table WHERE (name>=%s AND car_id=%s) OR (name<=%s AND car_id!=%s)")


    def test_build(self):
        """
        building the query results in a tuple of the sql and a list of values
        that need to be interpolated into the sql by Psycopg2.
        """
        q = Select('other_table', Person['name'] == 'Steve')
        self.assertEqual(q.build(),
                ('SELECT * FROM other_table WHERE name=%s',
                    ['Steve',]
                    )
                )

        q = Select('other_table', And(Person['name'] == 'Steve', Person['car_id'] == 12))
        self.assertEqual(q.build(),
                ('SELECT * FROM other_table WHERE name=%s AND car_id=%s',
                    ['Steve', 12]
                    )
                )

        q = Select('other_table', Or(
            And(Person['name'] == 'Steve', Person['car_id'] == 12),
            And(Person['name'] == 'Bob', Person['car_id'] == 1)
            )
            )
        self.assertEqual(q.build(),
                ('SELECT * FROM other_table WHERE (name=%s AND car_id=%s) OR (name=%s AND car_id=%s)',
                    ['Steve', 12, 'Bob', 1]
                    )
                )

        q = Select('other_table', Person['name'].Is('Bob'))
        self.assertEqual(q.build(),
                ('SELECT * FROM other_table WHERE name IS %s',
                ['Bob',])
                )
        q = Select('other_table', Person['name'].IsNot('Bob'))
        self.assertEqual(q.build(),
                ('SELECT * FROM other_table WHERE name IS NOT %s',
                ['Bob',])
                )
        q = Select('other_table', Person['name'].IsNull())
        self.assertEqual(q.build(),
                ('SELECT * FROM other_table WHERE name IS NULL',
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
                ('SELECT * FROM other_table WHERE name IS NULL AND foo=%s AND baz IS %s AND whatever IS DISTINCT FROM %s AND whatever IS NOT DISTINCT FROM %s',
                ['bar', 'bake', 'foo', 'bar'])
                )
        q = Select('other_table', Person['name'].IsNotNull())
        self.assertEqual(q.build(),
                ('SELECT * FROM other_table WHERE name IS NOT NULL',
                [])
                )


    def test_order_by(self):
        q = Select('other_table', Person['name'] == 'Steve').order_by('id ASC')
        self.assertEqual(q.build(),
                ('SELECT * FROM other_table WHERE name=%s ORDER BY id ASC',
                    ['Steve',]
                    )
                )


    def test_limit(self):
        q = Select('other_table', Person['name'] == 'Steve').order_by('id ASC'
                ).limit(12)
        self.assertEqual(q.build(),
                ('SELECT * FROM other_table WHERE name=%s ORDER BY id ASC LIMIT 12',
                    ['Steve',]
                    )
                )


    def test_offset(self):
        q = Select('other_table', Person['name'] == 'Steve').order_by('id ASC'
                ).offset(8)
        self.assertEqual(q.build(),
                ('SELECT * FROM other_table WHERE name=%s ORDER BY id ASC OFFSET 8',
                    ['Steve',]
                    )
                )
        q = Select('other_table', Person['name'] == 'Steve').order_by('id ASC'
                ).offset(8).limit(12)
        self.assertEqual(q.build(),
                ('SELECT * FROM other_table WHERE name=%s ORDER BY id ASC LIMIT 12 OFFSET 8',
                    ['Steve',]
                    )
                )


    def test_select_tuple(self):
        q = Select('cool_table', Person['id'].In((1,2)))
        self.assertEqual(q.build(),
                ('SELECT * FROM cool_table WHERE id IN %s',
                    [(1,2),]
                    )
                )

        q = Select('cool_table', Person['id'].In((1,2))).order_by('id DESC')
        self.assertEqual(q.build(),
                ('SELECT * FROM cool_table WHERE id IN %s ORDER BY id DESC',
                    [(1,2),]
                    )
                )



class TestInsert(unittest.TestCase):


    def test_basic(self):
        q = Insert('some_table', name='Bob')
        self.assertEqual(str(q), "INSERT INTO some_table (name) VALUES (%s)")
        q = Insert('some_table', name='Bob', id=3)
        self.assertEqual(str(q), "INSERT INTO some_table (id, name) VALUES (%s, %s)")
        q = Insert('some_table')
        self.assertEqual(str(q), "INSERT INTO some_table DEFAULT VALUES")


    def test_build(self):
        q = Insert('some_table', name='Bob')
        self.assertEqual(q.build(),
                ('INSERT INTO some_table (name) VALUES (%s)',
                    ['Bob',])
                )
        q = Insert('some_table', name='Bob', id=3)
        self.assertEqual(q.build(),
                ('INSERT INTO some_table (id, name) VALUES (%s, %s)',
                    [3, 'Bob'])
                )
        q = Insert('some_table', name='Bob', id=3, car_id=2)
        self.assertEqual(q.build(),
                ('INSERT INTO some_table (car_id, id, name) VALUES (%s, %s, %s)',
                    [2, 3, 'Bob'])
                )


    def test_returning(self):
        q = Insert('other_table', name='Bob').returning('*')
        self.assertEqual(q.build(),
                ('INSERT INTO other_table (name) VALUES (%s) RETURNING *',
                    ['Bob',])
                )



class TestUpdate(unittest.TestCase):


    def test_build(self):
        q = Update('some_table', name='Bob')
        self.assertEqual(q.build(), (
            "UPDATE some_table SET name=%s",
            ['Bob',]))

        q = Update('some_table', name='Bob', car_id=2)
        self.assertEqual(q.build(), (
            "UPDATE some_table SET car_id=%s, name=%s",
            [2, 'Bob']))

        q = Update('some_table', name='Bob', car_id=2).returning('*')
        self.assertEqual(q.build(), (
            "UPDATE some_table SET car_id=%s, name=%s RETURNING *",
            [2, 'Bob']))

        q = Update('some_table', name='Bob', car_id=2).where(
                Person['id']==3).returning('*')
        self.assertEqual(q.build(), (
            "UPDATE some_table SET car_id=%s, name=%s WHERE id=%s RETURNING *",
            [2, 'Bob', 3]))

        q = Update('some_table', name='Bob', car_id=2).where(
                And(Person['id']==3, Person['car_id']==4)).returning('*')
        self.assertEqual(q.build(), (
            'UPDATE some_table SET car_id=%s, name=%s WHERE id=%s AND car_id=%s RETURNING *',
            [2, 'Bob', 3, 4]))

        wheres = And()
        wheres.append(Person['id']==3)
        wheres.append(Person['car_id']==4)
        q = Update('some_table', name='Bob', car_id=2).where(wheres).returning('*')
        self.assertEqual(q.build(), (
            'UPDATE some_table SET car_id=%s, name=%s WHERE id=%s AND car_id=%s RETURNING *',
            [2, 'Bob', 3, 4]))



class TestDelete(unittest.TestCase):

    def test_build(self):
        q = Delete('some_table').where(Person['name']=='Bob')
        self.assertEqual(q.build(), (
            "DELETE FROM some_table WHERE name=%s",
            ['Bob',]))

        q = Delete('some_table').where(Person['name']>='Bob')
        self.assertEqual(q.build(), (
            "DELETE FROM some_table WHERE name>=%s",
            ['Bob',]))



if __name__ == '__main__':
    unittest.main()
