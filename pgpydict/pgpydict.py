"""
Access a Psycopg2 database as if it were a Python Dictionary.
"""

__all__ = ['DictDB', 'PgPyTable', 'PgPyDict', 'NoEntryError', 'NoPrimaryKey',
    '__version__']
__version__ = '0.1'

class NoEntryError(Exception): pass
class NoPrimaryKey(Exception): pass

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
        return join_str.join([str(i)+'=%('+str(i)+')s' for i in d])


def insert_column_value_pairs(d):
    """
    Create a string of SQL that will instruct a Psycopg2 DictCursor to
    interpolate the dictionary's keys into a INSERT SQL query.

    Example:
        >>> insert_column_value_pairs({'id':10, 'person':'Dave'})
        (id, person) VALUES (%(id)s, %(person)s)
    """
    return '({}) VALUES ({})'.format(
            ', '.join(sorted(d)),
            ', '.join(['%('+str(i)+')s' for i in sorted(d)]),
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
        self.order_by = None


    def _refresh_primary_keys(self):
        """
        Get a list of Primary Keys set for this table in the DB.
        """
        self.curs.execute('''SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '%s'::regclass
                AND i.indisprimary;''' % self.name)
        self.pks = [i[0] for i in self.curs.fetchall()]


    def __repr__(self):
        return 'PgPyTable({}, {})'.format(self.name, self.pks)


    def __call__(self, *a, **kw):
        d = PgPyDict(self, *a, **kw)
        return self._add_references(d)


    def _return_results(self, many=False):
        if self.curs.rowcount == 0:
            raise NoEntryError('No entry found')
        elif not many and self.curs.rowcount == 1:
            d = self(self.curs.fetchone())
            d._in_db = True
            return d
        l = [self(d) for d in self.curs.fetchall()]
        for i in l:
            i._in_db = True
        return l


    def _pk_value_pairs(self, join_str=' AND '):
        return column_value_pairs(self.pks, join_str)


    def get_where(self, *a, **kw):
        many = kw.pop('many', None)
        if a and len(a) == 1 and type(a[0]) == dict:
            # A single dictionary has been passed as an argument, use it as
            # the keyword arguments.
            kw = a[0]
        elif a:
            if not self.pks:
                raise NoPrimaryKey('No Primary Key(s) specified for '+str(self))
            kw = dict(zip(self.pks, a))
        elif not a and not kw:
            self.curs.execute('SELECT * FROM {}'.format(self.name))
            return self._return_results()

        self.curs.execute('SELECT * FROM {table} WHERE {wheres} ORDER BY {order_by}'.format(
                table=self.name,
                wheres=column_value_pairs(kw, ' AND '),
                order_by=self.order_by or self.pks[0],
            ),
            kw,
        )
        return self._return_results(many=many)


    def set_reference(self, my_column, key_name, pgpytable, their_column, many=False):
        self.refs[my_column] = (key_name, pgpytable, their_column, many)
        self.key_name_to_ref[key_name] = my_column


    def _add_references(self, d):
        for my_column in self.refs:
            key_name, pgpytable, their_column, many = self.refs[my_column]
            d[key_name] = None
        return d


    def count(self):
        self.curs.execute('SELECT COUNT(*) FROM {table}'.format(
            table=self.name))
        return self.curs.fetchone()[0]



class PgPyDict(dict):

    def __init__(self, pgpytable, *a, **kw):
        self._table = pgpytable
        self._in_db = False
        self._curs = pgpytable.db.curs
        super(PgPyDict, self).__init__(*a, **kw)


    def flush(self):
        """
        Insert this dictionary into it's table if its no yet in the Database,
        or Update it's row if it is already in the database.

        All column/values will bet inserted/set by this method.
        """
        if not self._in_db:
            self._curs.execute('INSERT INTO {table} {cvp} RETURNING *'.format(
                    table=self._table.name,
                    cvp=insert_column_value_pairs(self.remove_refs())
                ),
                self.remove_refs()
            )
            self._in_db = True
        else:
            if not self._table.pks:
                raise NoPrimaryKey('Cannot update to {}, no primary keys defined.'.format(
                    self._table))
            self._curs.execute('UPDATE {table} SET {cvp} WHERE {pvp} RETURNING *'.format(
                    table=self._table.name,
                    cvp=column_value_pairs(self.remove_refs()),
                    pvp=self._table._pk_value_pairs(),
                ),
                self
            )
        d = self._curs.fetchone()
        super(PgPyDict, self).__init__(d)
        return self


    def delete(self):
        """
        Delete this row from it's table in the database.
        """
        self._curs.execute('DELETE FROM {table} WHERE {pvp}'.format(
                table=self._table.name,
                pvp=self._table._pk_value_pairs()),
            self
            )


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
        """
        If the key being modified has a referenced pair, change that key's value
        to match the new row.
        """
        if key in self._table.refs:
            key_name, pgpytable, their_column, many = self._table.refs[key]
            # TODO What if there are multiple primary keys?
            d = pgpytable.get_where({self._table.pks[0]:value})
            super(PgPyDict, self).__setitem__(key_name, d)
        elif key in self._table.key_name_to_ref and type(value) == PgPyDict:
            super(PgPyDict, self).__setitem__(self._table.key_name_to_ref[key],
                    value[self._table.pks[0]])
        super(PgPyDict, self).__setitem__(key, value)


    def __getitem__(self, key):
        """

        """
        if key in self._table.key_name_to_ref:
            key_name, pgpytable, their_column, many = self._table.refs[self._table.key_name_to_ref[key]]
            super(PgPyDict, self).__setitem__(key,
                    pgpytable.get_where(
                        many=many,
                        **{their_column:self[self._table.key_name_to_ref[key]]})
                    )
        return super(PgPyDict, self).__getitem__(key)


    __setitem__.__doc__ += dict.__setitem__.__doc__
    __getitem__.__doc__ += dict.__getitem__.__doc__



