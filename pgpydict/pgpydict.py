from psycopg2.extras import DictCursor

__all__ = ['DictDB', 'PgPyTable', 'PgPyDict', 'NoEntryError', 'NoPrimaryKey']

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
        self.sort_by = None


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


    def __len__(self):
        self.curs.execute('SELECT COUNT(*) as "count" FROM {}'.format(self.name))
        return self.curs.fetchone()['count']


    def __call__(self, *a, **kw):
        d = PgPyDict(self, *a, **kw)
        return self._add_references(d)


    def _return_results(self, is_list=False):
        if self.curs.rowcount == 0:
            raise NoEntryError('No entry found')
        elif not is_list and self.curs.rowcount == 1:
            d = PgPyDict(self, self.curs.fetchone())
            d._in_db = True
            return self._add_references(d)
        l = [self._add_references(PgPyDict(self, d)) for d in self.curs.fetchall()]
        for i in l:
            i._in_db = True
        return l


    def _pk_value_pairs(self):
        return column_value_pairs(self.pks)


    def get_where(self, wheres=None, is_list=False):
        if type(wheres) == int:
            if not self.pks:
                raise NoPrimaryKey('No Primary Key(s) specified for '+str(self))
            wheres = {self.pks[0]:wheres,}
        elif wheres == None:
            self.curs.execute('SELECT * FROM {}'.format(self.name))
            return self._return_results()

        self.curs.execute('SELECT * FROM {} WHERE {} ORDER BY {}'.format(
                self.name,
                column_value_pairs(wheres, ' AND '),
                self.sort_by or self.pks[0],
            ),
            wheres,
        )
        return self._return_results(is_list=is_list)


    def set_reference(self, my_column, key_name, pgpytable, their_column, is_list=False):
        self.refs[my_column] = (key_name, pgpytable, their_column, is_list)
        self.key_name_to_ref[key_name] = my_column


    def _add_references(self, d):
        for my_column in self.refs:
            key_name, pgpytable, their_column, is_list = self.refs[my_column]
            d[key_name] = None
        return d



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
            if not self._table.pks:
                raise NoPrimaryKey('Cannot update to {}, no primary keys defined.'.format(
                    self._table))
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
        """
        If the key being modified has a referenced pair, change that key's value
        to match the new row.
        """
        if key in self._table.refs:
            key_name, pgpytable, their_column, is_list = self._table.refs[key]
            if len(pgpytable.pks) > 1:
                super().__setitem__(key_name, pgpytable.get_where(zip(self._table.pks, value)))
            else:
                super().__setitem__(key_name, pgpytable.get_where({self._table.pks[0]:value}))
        elif key in self._table.key_name_to_ref and type(value) == PgPyDict:
            super().__setitem__(self._table.key_name_to_ref[key],
                    value[self._table.pks[0]])
        super().__setitem__(key, value)


    def __getitem__(self, key):
        """

        """
        if key in self._table.key_name_to_ref:
            key_name, pgpytable, their_column, is_list = self._table.refs[self._table.key_name_to_ref[key]]
            super().__setitem__(key,
                    pgpytable.get_where(
                        {their_column:self[self._table.key_name_to_ref[key]]},
                        is_list=is_list)
                    )
        return super().__getitem__(key)


    __setitem__.__doc__ += dict.__setitem__.__doc__
    __getitem__.__doc__ += dict.__getitem__.__doc__



