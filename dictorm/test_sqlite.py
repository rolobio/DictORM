import unittest
from .sqlite import Select, Insert, Update, And, Column

class PersonTable(object):
    '''fake DictORM Table for testing'''

    name = 'person'

    def __getitem__(self, key):
        return Column(self, key)


Person = PersonTable()



class TestSqlite(unittest.TestCase):

    def test_comparisons(self):
        self.assertEqual(str(Person['name'] == 3), 'name=?')
        self.assertEqual(str(Person['name'] > 3), 'name>?')
        self.assertEqual(str(Person['name'] >= 3), 'name>=?')
        self.assertEqual(str(Person['name'] < 3), 'name<?')
        self.assertEqual(str(Person['name'] <= 3), 'name<=?')
        self.assertEqual(str(Person['name'] != 3), 'name!=?')


    def test_insert(self):
        self.assertEqual(str(Insert('whatever', name='foo')),
                'INSERT INTO whatever (name) VALUES (?)')
        self.assertEqual(str(Insert('whatever', name='foo', foo=3)),
                'INSERT INTO whatever (foo, name) VALUES (?, ?)')
        self.assertEqual(str(Insert('whatever', name='foo', foo=3, bar=3.2)),
                'INSERT INTO whatever (bar, foo, name) VALUES (?, ?, ?)')

        q = Insert('whatever').returning('foo')
        self.assertEqual(q.build(),
                [
                    ('INSERT INTO whatever DEFAULT VALUES', []),
                    ('SELECT foo FROM whatever WHERE rowid = last_insert_rowid()', [])
                ])


    def test_update(self):
        q = Update('whatever', foo='bar').where(
                And(Person['name']=='Steve', Person['id']==1)
                )
        self.assertEqual(q.build(),
                    ('UPDATE whatever SET foo=? WHERE name=? AND id=?',
                        ['bar', 'Steve', 1]))


    def test_select(self):
        q = Select('whatever', Person['name'] == 'foo')
        self.assertEqual(q.build(),
                (
                    'SELECT * FROM whatever WHERE name=?',
                    ['foo',]
                )
                )

        q = Select('whatever', And(Person['name'] == 'foo', Person['foo'] > 'bar'))
        self.assertEqual(q.build(),
                (
                    'SELECT * FROM whatever WHERE name=? AND foo>?',
                    ['foo', 'bar']
                )
                )




if __name__ == '__main__':
    unittest.main()
