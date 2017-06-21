import unittest
from dictorm.sqlite import Select, Insert, Update, And, Column
from dictorm.pg import set_sort_keys

class PersonTable(object):
    '''fake DictORM Table for testing'''

    name = 'person'

    def __getitem__(self, key):
        return Column(self, key)


Person = PersonTable()

set_sort_keys(True)



class TestSqlite(unittest.TestCase):

    def test_comparisons(self):
        self.assertEqual(str(Person['name'] == 3), '"person"."name"=?')
        self.assertEqual(str(Person['name'] > 3),  '"person"."name">?')
        self.assertEqual(str(Person['name'] >= 3), '"person"."name">=?')
        self.assertEqual(str(Person['name'] < 3),  '"person"."name"<?')
        self.assertEqual(str(Person['name'] <= 3), '"person"."name"<=?')
        self.assertEqual(str(Person['name'] != 3), '"person"."name"!=?')


    def test_insert(self):
        self.assertEqual(str(Insert('whatever', name='foo')),
                'INSERT INTO "whatever" ("name") VALUES (?)')
        self.assertEqual(str(Insert('whatever', name='foo', foo=3)),
                'INSERT INTO "whatever" ("foo", "name") VALUES (?, ?)')
        self.assertEqual(str(Insert('whatever', name='foo', foo=3, bar=3.2)),
                'INSERT INTO "whatever" ("bar", "foo", "name") VALUES (?, ?, ?)')

        q = Insert('whatever').returning('foo')
        self.assertEqual(q.build(),
                [
                    ('INSERT INTO "whatever" DEFAULT VALUES', []),
                    ('SELECT foo FROM "whatever" WHERE "rowid" = last_insert_rowid()', [])
                ])


    def test_update(self):
        q = Update('whatever', foo='bar').where(
                And(Person['name']=='Steve', Person['id']==1)
                )
        self.assertEqual(q.build(),
                    ('UPDATE "whatever" SET "foo"=? WHERE "person"."name"=? AND "person"."id"=?',
                        ['bar', 'Steve', 1]))


    def test_select(self):
        q = Select('whatever', Person['name'] == 'foo')
        self.assertEqual(q.build(),
                (
                    'SELECT * FROM "whatever" WHERE "person"."name"=?',
                    ['foo',]
                )
                )

        q = Select('whatever', And(Person['name'] == 'foo', Person['foo'] > 'bar'))
        self.assertEqual(q.build(),
                (
                    'SELECT * FROM "whatever" WHERE "person"."name"=? AND "person"."foo">?',
                    ['foo', 'bar']
                )
                )




if __name__ == '__main__':
    unittest.main()
