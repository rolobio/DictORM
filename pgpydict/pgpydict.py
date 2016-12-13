"""
Access a Psycopg2 database as if it were a Python Dictionary.
"""
from json import dumps
from copy import copy
from psycopg2.extras import DictCursor

__all__ = ['DictDB', 'PgPyTable', 'PgPyDict', 'NoEntryError', 'NoPrimaryKey',
    'UnexpectedRows', '__version__', 'column_value_pairs']
__version__ = '0.7'

class NoEntryError(Exception): pass
class NoPrimaryKey(Exception): pass
class UnexpectedRows(Exception): pass

def operator_kinds(o):
    if o in (tuple, list):
        return ' IN '
    return '='


def column_value_pairs(d, join_str=', ', prefix=''):
    """
    Create a string of SQL that will instruct a Psycopg2 DictCursor to
    interpolate the dictionary's keys into a SELECT or UPDATE SQL query.

    If old is True, prefix all values with old_ .  This is used to change
    primary key values.

    Example 1:
        >>> column_value_pairs({'id':10, 'person':'Dave'})
        id=%(id)s, person=%(person)s

    Example 2:
        >>> column_value_pairs(('id', 'person'))
        id=%(id)s, person=%(person)s

    Example 3:
        >>> column_value_pairs({'id':(10,11,13), 'group':'group'}, ' AND ')
        group=%(group)s AND id IN %(id)s

    Example 4:
        >>> column_value_pairs( {'id':12, 'person':'Dave'}, prefix='old_')
        id=%(old_id)s, person=%(old_person)s
    """
    if type(d) == dict:
        return join_str.join([
                str(k) + operator_kinds(type(d[k])) + '%('+prefix+k+')s'
                for k in sorted(d.keys())
            ])
    else:
        return join_str.join([str(i)+'=%('+prefix+str(i)+')s' for i in d])


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


def json_dicts(d):
    """
    Convert all dictionaries contained in this object into JSON strings.
    """
    for key, value in d.items():
        if type(value) == dict:
            d[key] = dumps(value)
    return d


class DictDB(dict):

    def __init__(self, psycopg2_conn):
        self.conn = psycopg2_conn
        self.curs = self.conn.cursor(cursor_factory=DictCursor)
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
        self.refname_to_mycolumn = {}
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


    def _return_results(self):
        while True:
            d = self.curs.fetchone()
            if not d:
                break
            # Convert returned dictionary to a PgPyDict
            d = self(d)
            d._in_db = True
            yield d


    def _pk_value_pairs(self, join_str=' AND ', prefix=''):
        return column_value_pairs(self.pks, join_str, prefix)


    def get_where(self, *a, **kw):
        order_by = None
        if self.order_by:
            order_by = self.order_by
        elif self.pks:
            order_by = self.pks[0]

        if a and len(a) == 1 and type(a[0]) == dict:
            # A single dictionary has been passed as an argument, use it as
            # the keyword arguments.
            kw = a[0]
        elif a:
            if not self.pks:
                raise NoPrimaryKey('No Primary Key(s) specified for '+str(self))
            kw = dict(zip(self.pks, a))
        elif not a and not kw:
            # No "wheres" provided, get all rows
            sql = 'SELECT * FROM {table} '
            if order_by:
                sql += 'ORDER BY {order_by}'
            self.curs.execute(sql.format(table=self.name, order_by=order_by))
            return self._return_results()

        sql = 'SELECT * FROM {table} WHERE {wheres} '
        if order_by:
            sql += 'ORDER BY {order_by}'
        self.curs.execute(sql.format(
                table=self.name,
                wheres=column_value_pairs(kw, ' AND '),
                order_by=order_by
            ),
            kw,
        )
        return self._return_results()


    def get_one(self, *a, **kw):
        l = list(self.get_where(*a, **kw))
        if len(l) > 1:
            raise UnexpectedRows('More than one row selected.')
        return l[0]


    def _add_references(self, d):
        for ref_name in self.refs:
            d[ref_name] = None
        return d


    def count(self):
        self.curs.execute('SELECT COUNT(*) FROM {table}'.format(
            table=self.name))
        return self.curs.fetchone()[0]


    def __setitem__(self, ref_name, value):
        my_column, pgpytable, their_column, many = value
        self.refs[ref_name] = (my_column, pgpytable, their_column, many)
        self.refname_to_mycolumn[ref_name] = ref_name


    def __getitem__(self, key):
        return Reference(self, key)



class Reference(object):
    """
    This class facilitates creating relationships between PgPyTables by using
    == and >.

    I would rather use "in" instead of ">", but "__contains__" overwrites what
    is returned and only returns a True/False value. :(
    """

    def __init__(self, pgpytable, column):
        self.pgpytable = pgpytable
        self.column = column

    def __eq__(self, reference):
        return (self.column, reference.pgpytable, reference.column, False)

    def __gt__(self, reference):
        return (self.column, reference.pgpytable, reference.column, True)



class PgPyDict(dict):

    def __init__(self, pgpytable, *a, **kw):
        self._table = pgpytable
        self._in_db = False
        self._curs = pgpytable.db.curs
        super(PgPyDict, self).__init__(*a, **kw)
        self._old = self.remove_refs()


    def flush(self):
        """
        Insert this dictionary into it's table if its no yet in the Database,
        or Update it's row if it is already in the database.

        All column/values will bet inserted/set by this method.
        """
        if not self._in_db:
            d = json_dicts(self.remove_refs())
            self._curs.execute('INSERT INTO {table} {cvp} RETURNING *'.format(
                    table=self._table.name,
                    cvp=insert_column_value_pairs(self.remove_refs())
                ),
                d
            )
            self._in_db = True
        else:
            if not self._table.pks:
                raise NoPrimaryKey('Cannot update to {}, no primary keys defined.'.format(
                    self._table))
            combined = copy(self.remove_refs())
            combined.update(dict([('old_'+k,v) for k,v in self._old.items()]))
            combined = json_dicts(combined)
            self._curs.execute('UPDATE {table} SET {cvp} WHERE {pvp} RETURNING *'.format(
                    table=self._table.name,
                    cvp=column_value_pairs(self.remove_refs()),
                    pvp=self._table._pk_value_pairs(prefix='old_'),
                ),
                combined
            )
        d = self._curs.fetchone()
        super(PgPyDict, self).__init__(d)
        self._old = self.remove_refs()
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
                if k not in self._table.refname_to_mycolumn
            ])


    def __getitem__(self, key):
        """
        Get the provided "key" from the dictionary.  If the key refers to a
        referenced row, get that row first.
        """
        if key in self._table.refs:
            my_column, pgpytable, their_column, many = self._table.refs[key]
            wheres = {their_column:self[my_column]}
            if many:
                val = pgpytable.get_where(**wheres)
            else:
                try:
                    val = pgpytable.get_one(**wheres)
                except IndexError:
                    # No results returned, must not be set
                    val = None
            super(PgPyDict, self).__setitem__(key, val)
        return super(PgPyDict, self).__getitem__(key)


    __getitem__.__doc__ += dict.__getitem__.__doc__



