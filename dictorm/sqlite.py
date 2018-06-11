"""
Provide Sqlite3 support by making simple changes to dictorm.pg classes.
"""
from dictorm import pg

Select = pg.Select


class Comparison(pg.Comparison):
    interpolation_str = '?'


class Column(pg.Column):
    comparison = Comparison


class Insert(pg.Insert):
    interpolation_str = '?'

    def returning(self, returning):
        self.append_returning = returning
        return self


class Update(pg.Update):
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
