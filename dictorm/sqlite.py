'''
Provide Sqlite3 support by making simple changes to dictorm.pg classes.
'''
from .pg import Column as PostgresqlColumn
from .pg import Comparison as PostgresqlComparison
from .pg import Insert as PostgresqlInsert
from .pg import Select, And
from .pg import Update as PostgresqlUpdate


class Comparison(PostgresqlComparison):
    interpolation_str = '?'


class Column(PostgresqlColumn):
    comparison = Comparison


class Insert(PostgresqlInsert):
    interpolation_str = '?'

    def returning(self, returning):
        self.append_returning = returning
        return self


class Update(PostgresqlUpdate):
    interpolation_str = '?'

    def returning(self, returning):
        self.append_returning = returning
        return self

    def build(self):
        # Replace the last_insert_rowid select with one built around this query
        built = super(Update, self).build()
        if self.append_returning:
            built[1] = Select(self.table, self.operators_or_comp).build()
        return built
