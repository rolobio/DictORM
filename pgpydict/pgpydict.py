from psycopg2.extras import DictCursor

__all__ = ['DictDB']

class NoEntryError(Exception): pass

def operator_kinds(o):
    if o in (tuple, list):
        return ' IN '
    return '='


def column_value_pairs(d, join_str=', '):
    """
    Create a string of SQL that will instruct a Psycopg2 DictCursor to
    interpolate the dictionary's keys into a SELECT or UPDATE SQL query.

    Example:
        d = {'id':10, 'person':'Dave'}

        becomes

        id=%(id)s, person=%(person)s
    """
    return join_str.join([
            str(k) + operator_kinds(type(d[k])) + '%('+k+')s'
            for k in d.keys()
        ])


def insert_column_value_pairs(d):
    """
    Create a string of SQL that will instruct a Psycopg2 DictCursor to
    interpolate the dictionary's keys into a INSERT SQL query.

    Example:
        d = {'id':10, 'person':'Dave'}

        becomes

        (id, person) VALUES (%(id)s, %(person)s)
    """
    return '({}) VALUES ({})'.format(
            ', '.join(d),
            ', '.join(['%('+k+')s' for k in d]),
            )


global UPDATES; UPDATES = []


class DictDB(dict):

    def __init__(self, cursor):
        self.curs = cursor
        self.refresh_tables()
        super(DictDB, self).__init__()


    def _list_tables(self):
        self.curs.execute('''SELECT DISTINCT table_name
                FROM information_schema.columns
                WHERE table_schema='public' ''')
        return self.curs.fetchall()


    def refresh_tables(self):
        if self.keys():
            # Reset this DictDB because it contains old tables
            super(DictDB, self).__init__()
        for table in self._list_tables():
            self[table['table_name']] = PgPyTable(table['table_name'], self)



class PgPyTable(object):

    def __init__(self, table, db):
        self.table = table
        self.db = db
        self.execute = db.curs.execute


    def __repr__(self):
        return 'PgPyTable({})'.format(self.table)


    def __len__(self):
        self.execute('SELECT COUNT(*) as "count" FROM {}'.format(self.table))
        return self.db.curs.fetchone()['count']


    def __call__(self, *a, **kw):
        return PgPyDict(self, *a, **kw)



class PgPyDict(dict):

    def __init__(self, pgpytable, *a, **kw):
        self._table = pgpytable
        self._in_db = False
        self._execute = pgpytable.execute
        super().__init__(*a, **kw)


    def flush(self):
        if not self._in_db:
            self._execute('INSERT INTO {} {} RETURNING *'.format(
                self._table.table,
                insert_column_value_pairs(self)
                ),
                self
            )
        d = self._table.db.curs.fetchone()
        super().__init__(d)
        return self



