'''
Provide Sqlite3 support by making simple changes to dictorm.query classes.
'''
from dictorm.query import *

class Expression(Expression):

    interpolation_str = '?'


class Insert(Insert):

    interpolation_str = '?'

    def returning(self, returning):
        self.append_returning = returning
        return self



class Update(Update):

    interpolation_str = '?'


class Column(object):

    def __init__(self, table, column):
        self.table = table
        self.column = column

    def __repr__(self):
        return 'Column({}.{})'.format(self.table, self.column)

    # These aren't duplicates, they're using the newly defined Expression above
    def __eq__(self, column): return Expression(self, column, '=')
    def __gt__(self, column): return Expression(self, column, '>')
    def __ge__(self, column): return Expression(self, column, '>=')
    def __lt__(self, column): return Expression(self, column, '<')
    def __le__(self, column): return Expression(self, column, '<=')
    def __ne__(self, column): return Expression(self, column, '!=')

    def In(self, tup): return Expression(self, tup, ' IN ')



