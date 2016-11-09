from psycopg2.extras import DictCursor

__all__ = ['DictDB', 'PgPyTable', 'PgPyDict']

class NoEntryError(Exception): pass

def operator_kinds(o):
    if o in (tuple, list):
        return ' IN '
    return '='


def column_value_pairs(d, join_str=', '):
    """
    Create a string of SQL that will instruct a Psycopg2 DictCursor to
    interpolate the dictionary's keys into a SELECT or UPDATE SQL query.

    Example 1:
        >>> column_value_pairs({'id':10, 'person':'Dave'})
        id=%(id)s, person=%(person)s

    Example 2:
        >>> column_value_pairs(('id', 'person'))
        id=%(id)s, person=%(person)s

    Example 3:
        >>> column_value_pairs({'id':(10,11,13), 'group':'foo'}, ' AND ')
        id IN %(id)s AND group=%(foo)s
    """
    if type(d) == dict:
        return join_str.join([
                str(k) + operator_kinds(type(d[k])) + '%('+k+')s'
                for k in d.keys()
            ])
    else:
        return join_str.join([i+'=%('+i+')s' for i in d])


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

    def __init__(self, table_name, db):
        self.name = table_name
        self.db = db
        self.curs = db.curs
        self.pks = []
        self.refs = {}
        self.key_name_to_ref = {}
        self._refresh_primary_keys()


    def _refresh_primary_keys(self):
        """
        Get a list of Primary Keys set for this table in the DB.
        """
        self.curs.execute('''SELECT a.attname AS data_type
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '%s'::regclass
                AND i.indisprimary;''' % self.name)
        self.pks = [i[0] for i in self.curs.fetchall()]


    def __repr__(self):
        return 'PgPyTable({}, {})'.format(self.name, self.pks)


    def __len__(self):
        self.curs.execute('SELECT COUNT(*) as "count" FROM {}'.format(self.name))
        return self.curs.fetchone()['count']


    def __call__(self, *a, **kw):
        return PgPyDict(self, *a, **kw)


    def _return_results(self):
        if self.curs.rowcount == 0:
            return None
        elif self.curs.rowcount == 1:
            d = PgPyDict(self, self.curs.fetchone())
            d._in_db = True
            return d
        l = [PgPyDict(self, d) for d in self.curs.fetchall()]
        for i in l:
            i._in_db = True
        return l


    def _pk_value_pairs(self):
        return column_value_pairs(self.pks)


    def get_where(self, wheres):
        if type(wheres) == int:
            wheres = {self.pks[0]:wheres,}
        self.curs.execute('SELECT * FROM {} WHERE {}'.format(
                self.name,
                column_value_pairs(wheres, ' AND ')
            ),
            wheres
        )
        return self._return_results()


    def set_reference(self, my_column, key_name, pgpytable, their_column):
        self.refs[my_column] = (key_name, pgpytable, their_column)
        self.key_name_to_ref[key_name] = my_column



class PgPyDict(dict):

    def __init__(self, pgpytable, *a, **kw):
        self._table = pgpytable
        self._in_db = False
        self._curs = pgpytable.db.curs
        super().__init__(*a, **kw)


    def flush(self):
        """
        Insert this dictionary into it's table if its no yet in the Database,
        or Update it's row if it is already in the database.

        All column/values will bet inserted/set by this method.
        """
        if not self._in_db:
            self._curs.execute('INSERT INTO {} {} RETURNING *'.format(
                    self._table.name,
                    insert_column_value_pairs(self.remove_refs())
                ),
                self.remove_refs()
            )
            self._in_db = True
        else:
            self._curs.execute('UPDATE {} SET {} WHERE {} RETURNING *'.format(
                    self._table.name,
                    column_value_pairs(self.remove_refs()),
                    self._table._pk_value_pairs(),
                ),
                self
            )
        d = self._curs.fetchone()
        super().__init__(d)
        return self


    def remove_pks(self):
        """
        Return a dictionary without the primary keys that are associated with
        this PgPyDict in the Database.  This should be used when doing an update
        of another PgPyDict.
        """
        return dict([(k,v) for k,v in self.items() if k not in self._table.pks])


    def remove_refs(self):
        """
        Return a dictionary without the key/value(s) added by a reference.  They
        should never be sent in the query to the Database.
        """
        return dict([
            (k,v) for k,v in self.items()
                if k not in self._table.key_name_to_ref
            ])


    def __setitem__(self, key, value):
        if key in self._table.refs:
            key_name, pgpytable, their_column = self._table.refs[key]
            if len(pgpytable.pks) > 1:
                super().__setitem__(key_name, pgpytable.get_where(zip(self._table.pks, value)))
            else:
                super().__setitem__(key_name, pgpytable.get_where({self._table.pks[0]:value}))
        elif key in self._table.key_name_to_ref:
            super().__setitem__(self._table.key_name_to_ref[key],
                    value[self._table.pks[0]])
        super().__setitem__(key, value)


