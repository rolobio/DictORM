'''
Provide Sqlite3 support by making simple changes to dictorm.pg classes.
'''
from .pg import And
from .pg import Column as PostgresqlColumn
from .pg import Comparison as PostgresqlComparison
from .pg import Insert as PostgresqlInsert
from .pg import Select
from .pg import Update as PostgresqlUpdate

class Comparison(PostgresqlComparison): interpolation_str = '?'
class Update(PostgresqlUpdate): interpolation_str = '?'
class Column(PostgresqlColumn): comparison = Comparison


class Insert(PostgresqlInsert):

    interpolation_str = '?'

    def returning(self, returning):
        self.append_returning = returning
        return self



