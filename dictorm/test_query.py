import unittest
from dictorm.query import (Select, Insert,
        Or, Xor, And, Column)

class PersonTable(object):
    '''fake DictORM Table for testing'''

    name = 'person'

    def __getitem__(self, key):
        return Column(self, key)


Person = PersonTable()


class TestSelect(unittest.TestCase):


    def test_basic(self):
        q = Select('some_table', Person['name'] == 'Bob')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name='Bob'")
        q = Select('some_table', 'Bob' == Person['name'])
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name='Bob'")
        q = Select('some_table', Person['name'] > 'Bob')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name>'Bob'")
        q = Select('some_table', Person['name'] >= 'Bob')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name>='Bob'")
        q = Select('some_table', Person['name'] < 'Bob')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name<'Bob'")
        q = Select('some_table', Person['name'] <= 'Bob')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name<='Bob'")
        q = Select('some_table', Person['name'] != 'Bob')
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name!='Bob'")


    def test_logical(self):
        bob_name = Person['name'] == 'Bob'
        bob_car = Person['car_id'] == 2
        q = Select('some_table', bob_name.Or(bob_car))
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name='Bob' OR car_id=2")
        q = Select('some_table', bob_name.And(bob_car))
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name='Bob' AND car_id=2")
        q = Select('some_table', bob_name.Xor(bob_car))
        self.assertEqual(str(q), "SELECT * FROM some_table WHERE name='Bob' XOR car_id=2")


    def test_logical_groups(self):
        q = Select('some_table', Or(
            And(Person['name'] == 'Bob', Person['car_id'] == 2),
            Person['name'] == 'Alice'
            ))

        self.assertEqual(str(q),
                "SELECT * FROM some_table WHERE (name='Bob' AND car_id=2) OR name='Alice'")

        q = Select('some_table', Or(
            And(Person['name'] >= 'Bob', Person['car_id'] == 2),
            And(Person['name'] <= 'Alice', Person['car_id'] != 3)
            ))
        self.assertEqual(str(q),
                "SELECT * FROM some_table WHERE (name>='Bob' AND car_id=2) OR (name<='Alice' AND car_id!=3)")



class TestInsert(unittest.TestCase):


    def test_basic(self):
        q = Insert('some_table', name='Bob')
        self.assertEqual(str(q), "INSERT INTO some_table (name) VALUES ('Bob')")


if __name__ == '__main__':
    unittest.main()
