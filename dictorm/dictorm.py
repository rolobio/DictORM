"""
Access a Psycopg2 database as if it were a Python Dictionary.
"""
from json import dumps
from psycopg2.extras import DictCursor

__all__ = ['DictDB', 'PgTable', 'PgDict', 'NoEntryError', 'NoPrimaryKey',
    'UnexpectedRows', 'ResultsGenerator', '__version__', 'column_value_pairs']
__version__ = '1.2'

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
    d = sorted(d)
    return '({}) VALUES ({})'.format(
            ', '.join(d),
            ', '.join(['%('+str(i)+')s' for i in d]),
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
    """
    Get all the tables from the provided psycopg2 connection.  Create a
    PgTable for that table, and keep it in this instance using the table's
    name as a key.

    >>> db =DictDB(your_db_connection)
    >>> db['table1']
    PgTable('table1')

    >>> db['other_table']
    PgTable('other_table')

    If your tables have changed while your DictDB instance existed, you can call
    DictDB.refresh_tables() to have it rebuild all PgTable objects.
    """

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
            self[table['table_name']] = PgTable(table['table_name'], self)



class ResultsGenerator:
    """
    This class replicates a Generator, it mearly adds the ability to
    get the len() of the generator (the rowcount of the last query run).
    This method should only be returned by PgTable.get_where and
    PgTable.get_one.

    Really, just use this class as if it were a generator unless you want
    a count.
    """

    def __init__(self, query, vars, pgpytable):
        self.query = query
        self.vars = vars
        self.pgpytable = pgpytable
        # This needs its own generator in case the usual cursor is used to
        # Update/Delete/Insert, overwriting the results of this query.
        self.curs = pgpytable.db.conn.cursor(cursor_factory=DictCursor)

    def __iter__(self): return self

    def __next__(self):
        if self.query:
            # Run the query only once
            self.curs.execute(self.query, self.vars)
            self.query = None

        d = self.curs.fetchone()

        if not d:
            raise StopIteration
        # Convert returned dictionary to a PgDict
        d = self.pgpytable(d)
        d._in_db = True
        return d


    # for python 2.7
    next = __next__


    def __len__(self):
        if self.query:
            # Run the query only once
            self.curs.execute(self.query, self.vars)
            self.query = None
        return self.curs.rowcount



class PgTable(object):
    """
    A representation of a Postgresql table.  You will primarily retrieve
    rows (PgDicts) from the database using the PgTable.get_where method.

    Get all rows that need to be updates:

    >>> list(table.get_where(outdated=True))
    [PgDict(), PgDict(), PgDict(), PgDict()]

    Get a single row (will raise an UnexpectedRow error if more than one
    row could have been returned):

    >>> table.get_one(id=12)
    PgDict()
    >>> table.get_one(manager_id=14)
    PgDict()

    You can reference another table using setitem, link to an employee's
    manager using the manager's id, and the employee's manager_id.

    >>> person['manager'] = person['manager_id'] == person['id']
    >>> person['manager']
    PgDict()

    Reference a manager's subordinates using their collective manager_id's.:
    (Use > instead of "in" because __contains__'s value is overwritten by
    python)

    >>> person['subordinates'] = person['id'] > person['manager_id']
    >>> list(person['manager'])
    [PgDict(), PgDict()]

    PgTable.get_where returns a generator object, this makes it so you
    won't have an entire table's object in memory at once, they are
    generated when gotten:

    >>> person['subordinates']
    ResultsGenerator()
    >>> for sub in person['subordinates']:
    >>>     print(sub)
    PgDict()
    PgDict()
    PgDict()

    Get a count of all rows in this table:

    >>> person.count()
    3
    """

    def __init__(self, table_name, db):
        self.name = table_name
        self.db = db
        self.curs = db.curs
        self.pks = []
        self.refs = {}
        self._set_pks()
        self.order_by = None


    def _set_pks(self):
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


    def __repr__(self): # pragma: no cover
        return 'PgTable({}, {})'.format(self.name, self.pks)


    def __call__(self, *a, **kw):
        d = PgDict(self, *a, **kw)
        return self._add_references(d)


    def _pk_value_pairs(self, join_str=' AND ', prefix=''):
        return column_value_pairs(self.pks, join_str, prefix)


    def get_where(self, *a, **kw):
        """
        Get all rows as PgDicts where values are as specified.  This always
        returns a generator-like object ResultsGenerator.  You can get the
        length of that generator see ResultsGenerator.count.

        If you provide only arguments, they will be paired in their respective
        order to the primary keys defined for this table.  If the primary keys
        of this table was (id,) only:

            get_where(4) is equal to get_where(id=4)

            get_where(4, 5) would raise a NoPrimaryKey error because there is
                            only one primary key.

        Primary keys are defined automatically by during the init of the object,
        but you can overwrite that by simply changing the value:

        >>> your_table.pks = ['id', 'some_column', 'whatever_you_want']

            get_where(4, 5, 6) is now equal to get_where(id=4, some_column=5,
                                                    whatever_you_want=6)

        If there were two primary keys, such as in a join table (id, group):

            get_where(4, 5) is equal to get_where(id=4, group=5)

        You cannot use this method without primary keys, unless you specify
        the column you are matching.

        >>> get_where(some_column=83)
        ResultsGenerator()

        >>> get_where(4) # no primary keys defined!
        NoPrimaryKey()

        """
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

        # Build out the query using user-provideded data, and data gathered
        # from the DB.
        sql = 'SELECT * FROM {table} '
        if kw:
            sql += 'WHERE {wheres} '
        if order_by:
            sql += 'ORDER BY {order_by}'
        sql = sql.format(
                table=self.name,
                wheres=column_value_pairs(kw, ' AND '),
                order_by=order_by
            )
        return ResultsGenerator(sql, kw, self)


    def get_one(self, *a, **kw):
        """
        Get a single row as a PgDict from the Database that matches provided
        to this method.  See PgTable.get_where for more details.

        If more than one row could be returned, this will raise an
        UnexpectedRows error.
        """
        l = list(self.get_where(*a, **kw))
        if len(l) > 1:
            raise UnexpectedRows('More than one row selected.')
        return l[0]


    def _add_references(self, d):
        for ref_name in self.refs:
            d[ref_name] = None
        return d


    def count(self):
        """
        Get the count of rows in this table.
        """
        self.curs.execute('SELECT COUNT(*) FROM {table}'.format(
            table=self.name))
        return int(self.curs.fetchone()[0])


    def __setitem__(self, ref_name, value):
        if len(value) == 3:
            my_column, sub_reference, their_refname = value
            self.refs[ref_name] = (my_column, sub_reference, their_refname)
        else:
            my_column, pgpytable, their_column, many = value
            self.refs[ref_name] = (self, my_column, pgpytable, their_column, many)


    def __getitem__(self, key):
        return Reference(self, key)



class Reference(object):
    """
    This class facilitates creating relationships between PgTables by using
    == and >.

    I would rather use "in" instead of ">", but "__contains__" overwrites what
    is returned and only returns a True/False value. :(
    """

    def __init__(self, pgpytable, column):
        self.pgpytable = pgpytable
        self.column = column

    def __repr__(self): # pragma: no cover
        return 'Reference({}, {})'.format(self.pgpytable.name, self.column)

    def __eq__(ref1, ref2):
        return (ref1.column, ref2.pgpytable, ref2.column, False)

    def __gt__(ref1, ref2):
        return (ref1.column, ref2.pgpytable, ref2.column, True)

    def subReference(self, column):
        return (self.column, self.pgpytable[self.column], column)



class PgDict(dict):
    """
    This behaves exactly like a dictionary, you may update your database row
    (this PgDict instance) using update or simply by setting an item.  After
    you make changes, be sure to call PgDict.flush on the instance of this
    object.

    This relies heavily on primary keys and they should be specified.  Really,
    your tables should have a primary key of some sort.  If not, this will
    pretty much be a read-only object.

    You can change the primary key of an instance.

    Use setitem:
    >>> d['manager_id'] = 4

    Use an update:
    >>> d.update({'manager_id':4})

    Update using another PgDict:
    >>> d1.update(d2.remove_pks())

    Make sure to send your changes to the database:
    >>> d.flush()

    Remove a row:
    >>> d.remove()
    """

    def __init__(self, pgpytable, *a, **kw):
        self._table = pgpytable
        self._in_db = False
        self._curs = pgpytable.db.curs
        super(PgDict, self).__init__(*a, **kw)
        self._old = self.remove_refs()


    def flush(self):
        """
        Insert this dictionary into it's table if its no yet in the Database,
        or Update it's row if it is already in the database.  This method
        relies heaviliy on the primary keys of the row's respective table.  If
        no primary keys are specified, this method will not function!

        All original column/values will bet inserted/set by this method.  If
        a reference sub-dictionary has been defined, it will NOT be submitted to
        the DB.  However, the reference's respective reference column will be
        updated.
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
            combined = self.remove_refs()
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
        super(PgDict, self).__init__(d)
        self._old = self.remove_refs()
        return self


    def delete(self):
        """
        Delete this row from it's table in the database.  Requires primary
        keys to be specified.
        """
        self._curs.execute('DELETE FROM {table} WHERE {pvp}'.format(
                table=self._table.name,
                pvp=self._table._pk_value_pairs()),
            self
            )


    def remove_pks(self):
        """
        Return a dictionary without the primary keys that are associated with
        this PgDict in the Database.  This should be used when doing an update
        of another PgDict.
        """
        return dict([(k,v) for k,v in self.items() if k not in self._table.pks])


    def remove_refs(self):
        """
        Return a dictionary without the key/value(s) added by a reference.  They
        should never be sent in the query to the Database.
        """
        return dict([(k,v) for k,v in self.items() if k not in self._table.refs])


    def __getitem__(self, key):
        """
        Get the provided "key" from the dictionary.  If the key refers to a
        referenced row, get that row first.
        """
        ref = self._table.refs.get(key)
        sub_reference = False
        if ref:
            if len(ref) == 3:
                sub_reference = True
                # This reference is linking two references, get the value of the
                # regular reference using usual means, then pull the
                # sub-reference.
                my_column, pgpytable, their_sub_ref = ref
                ref = self._table.refs[my_column]

            my_table, my_column, pgpytable, their_column, many = ref
            wheres = {their_column:self[my_column]}
            if many:
                val = pgpytable.get_where(**wheres)
            else:
                try:
                    val = pgpytable.get_one(**wheres)
                except IndexError:
                    # No results returned, must not be set
                    val = None

            if sub_reference and many:
                val = [i[their_sub_ref] for i in val]
            elif sub_reference:
                val = val[their_sub_ref]

            super(PgDict, self).__setitem__(key, val)
            return val
        return super(PgDict, self).__getitem__(key)


    __getitem__.__doc__ += dict.__getitem__.__doc__



