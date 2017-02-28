'''
Provide Sqlite3 support by making simple changes to dictorm.pg classes.
'''
from .pg import And
from .pg import Column as PostgresqlColumn
from .pg import Expression as PostgresqlExpression
from .pg import Insert as PostgresqlInsert
from .pg import Select
from .pg import Update as PostgresqlUpdate

class Expression(PostgresqlExpression): interpolation_str = '?'
class Update(PostgresqlUpdate): interpolation_str = '?'

class Column(PostgresqlColumn):

    # These aren't duplicates, they're using the newly defined Expression above
    def __eq__(self, column): return Expression(self, column, '=')
    def __gt__(self, column): return Expression(self, column, '>')
    def __ge__(self, column): return Expression(self, column, '>=')
    def __lt__(self, column): return Expression(self, column, '<')
    def __le__(self, column): return Expression(self, column, '<=')
    def __ne__(self, column): return Expression(self, column, '!=')
    def Is(self, column):     return Expression(self, column, ' IS ')
    def IsNot(self, column):  return Expression(self, column, ' IS NOT ')

    def In(self, tup): return Expression(self, tup, ' IN ')



class Insert(PostgresqlInsert):

    interpolation_str = '?'

    def returning(self, returning):
        self.append_returning = returning
        return self



