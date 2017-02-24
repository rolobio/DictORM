'''
Provide Sqlite3 support by making simple changes to dictorm.query classes.
'''
try: # pragma: no cover
    from dictorm.query import And as PostgresqlAnd
    from dictorm.query import Column as PostgresqlColumn
    from dictorm.query import Expression as PostgresqlExpression
    from dictorm.query import Insert as PostgresqlInsert
    from dictorm.query import Select as PostgresqlSelect
    from dictorm.query import Update as PostgresqlUpdate
except ImportError: # pragma: no cover
    from .query import And as PostgresqlAnd
    from .query import Column as PostgresqlColumn
    from .query import Expression as PostgresqlExpression
    from .query import Insert as PostgresqlInsert
    from .query import Select as PostgresqlSelect
    from .query import Update as PostgresqlUpdate

class And(PostgresqlAnd): pass
class Expression(PostgresqlExpression): interpolation_str = '?'
class Select(PostgresqlSelect): pass
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



