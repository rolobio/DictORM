"""
What if you could insert a Python dictionary into the database?  DictORM allows
you to select/insert/update rows of a database as if they were Python
Dictionaries.
"""
__version__ = '3.1.3'

from sys import modules
from json import dumps
from copy import deepcopy

try: # pragma: no cover
    from dictorm.pg import Select, Insert, Update, Delete, And
    from dictorm.pg import Column, Comparison, Operator
    from dictorm.sqlite import Insert as SqliteInsert
    from dictorm.sqlite import Column as SqliteColumn
    from dictorm.sqlite import Update as SqliteUpdate
except ImportError: # pragma: no cover
    from .pg import Select, Insert, Update, Delete, And
    from .pg import Column, Comparison, Operator
    from .sqlite import Insert as SqliteInsert
    from .sqlite import Column as SqliteColumn
    from .sqlite import Update as SqliteUpdate

db_package_imported = False
try: # pragma: no cover
    from psycopg2.extras import DictCursor
    db_package_imported = True
except ImportError: # pragma: no cover
    pass

try: # pragma: no cover
    import sqlite3
    db_package_imported = True
except ImportError: # pragma: no cover
    pass

if not db_package_imported: # pragma: no cover
    raise ImportError('Failed to import psycopg2 or sqlite3.  These are the only supported Databases and you must import one of them')


class NoPrimaryKey(Exception): pass
class UnexpectedRows(Exception): pass


def json_dicts(d):
    """
    Convert all dictionaries contained in this object into JSON strings.
    """
    for key, value in d.items():
        if isinstance(value, dict):
            d[key] = dumps(value)
    return d


class DictDB(dict):
    """
    Get all the tables from the provided Psycopg2/Sqlite3 connection.  Create a
    Table for each table, and keep tehm in this DictDB using the table's
    name as a key.

    >>> db = DictDB(your_db_connection)
    >>> db['table1']
    Table('table1')

    >>> db['other_table']
    Table('other_table')

    If your tables have changed while your DictDB instance existed, you can call
    DictDB.refresh_tables() to have it rebuild all Table objects.
    """

    def __init__(self, db_conn):
        self.conn = db_conn
        if 'sqlite3' in modules and isinstance(db_conn, sqlite3.Connection):
            self.kind = 'sqlite3'
        else:
            self.kind = 'postgresql'

        if self.kind == 'sqlite3':
            # row_factory using builtin Row which acts like a dictionary
            self.conn.row_factory = sqlite3.Row
            self.curs = self.conn.cursor()
        elif self.kind == 'postgresql':
            # using builtin DictCursor which gets/inserts/updates using
            # dictionaries
            self.curs = self.conn.cursor(cursor_factory=DictCursor)

        self.refresh_tables()
        super(DictDB, self).__init__()


    def _list_tables(self):
        if self.kind == 'sqlite3':
            self.curs.execute('SELECT name FROM sqlite_master WHERE type = "table"')
        else:
            self.curs.execute('''SELECT DISTINCT table_name
                    FROM information_schema.columns
                    WHERE table_schema='public' ''')
        return self.curs.fetchall()


    def refresh_tables(self):
        if self.keys():
            # Reset this DictDB because it contains old tables
            super(DictDB, self).__init__()
        for table in self._list_tables():
            if self.kind == 'sqlite3':
                self[table['name']] = Table(table['name'], self)
            else:
                self[table['table_name']] = Table(table['table_name'], self)



class ResultsGenerator:
    """
    This class replicates a Generator, it mearly adds the ability to get the
    len() of the generator (the rowcount of the last query run).  This method
    should only be returned by Table.get_where and Table.get_one.  It will
    cache the values gotten by "next" and once all rows have been fetched,
    it will continue to return that cache.

    Really, just use this class as if it were a generator unless you want
    a count.
    """

    def __init__(self, table, query, db):
        self.table = table
        self.query = query
        self.cache = []
        self.completed = False
        self.refined = False
        self.executed = False
        self.db_kind = db.kind
        self.db = db


    def _new_cursor(self):
        # This needs its own generator in case the usual cursor is used to
        # Update/Delete/Insert, overwriting the results of this query.
        if self.db.kind == 'sqlite3':
            self.curs = self.db.conn.cursor()
        elif self.db.kind == 'postgresql':
            self.curs = self.db.conn.cursor(cursor_factory=DictCursor)


    def __iter__(self):
        if self.completed:
            return iter(self.cache)
        else:
            return self


    def __next__(self):
        self._execute_once()
        d = self.curs.fetchone()
        if not d:
            self.completed = True
            raise StopIteration
        # Convert returned dictionary to a Dict
        d = self.table(d)
        d._in_db = True
        self.cache.append(d)
        return d


    def _execute_once(self):
        """
        Execute the query only once
        """
        if not self.executed:
            self._new_cursor()
            self.executed = True
            sql, values = self.query.build()
            self.curs.execute(sql, values)


    # for python 2.7
    next = __next__


    def __len__(self):
        self._execute_once()
        if self.db_kind == 'sqlite3':
            # sqlite3's cursor.rowcount doesn't support select statements
            return 0
        return self.curs.rowcount


    def refine(self, *a, **kw):
        query = deepcopy(self.query)
        for exp in a:
            query.append(exp)
        for k,v in kw.items():
            query.append(self.table[k]==v)
        return ResultsGenerator(self.table, query, self.db)


    def order_by(self, order_by):
        query = deepcopy(self.query).order_by(order_by)
        return ResultsGenerator(self.table, query, self.db)


    def limit(self, limit):
        query = deepcopy(self.query).limit(limit)
        return ResultsGenerator(self.table, query, self.db)


    def offset(self, offset):
        query = deepcopy(self.query).offset(offset)
        return ResultsGenerator(self.table, query, self.db)



class Table(object):
    """
    A representation of a DB table.  You will primarily retrieve rows
    (Dicts) from the database using the get_where and get_one methods.

    Insert into this table:

    >>> your_table(some_column='some value', other=False)
    {'some_column':'some value', 'other':False}

    Get all rows that need to be updated:

    >>> list(table.get_where(outdated=True))
    [Dict(), Dict(), Dict(), Dict()]

    Get a single row (will raise an UnexpectedRow error if more than one
    row could have been returned):

    >>> table.get_one(id=12)
    Dict()
    >>> table.get_one(manager_id=14)
    Dict()

    You can reference another table using setitem.  Link to an employee's
    manager using the manager's id, and the employee's manager_id.

    >>> Person['manager'] = Person['manager_id'] == Person['id']
    >>> bob['manager']
    Dict()

    Reference a manager's subordinates using their collective manager_id's:

    >>> Person['subordinates'] = Person['id'].many(Person['manager_id'])
    >>> list(bob['manager'])
    [Dict(), Dict()]

    Table.get_where returns a generator object, this makes it so you
    won't have an entire table's object in memory at once, they are
    generated when gotten:

    >>> bob['subordinates']
    ResultsGenerator()
    >>> for sub in bob['subordinates']:
    >>>     print(sub)
    Dict()
    Dict()
    Dict()

    Get a count of all rows in this table:

    >>> Person.count()
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
        self.fks = {}


    def _set_pks(self):
        """
        Get a list of Primary Keys set for this table in the DB.
        """
        if self.db.kind == 'sqlite3':
            self.curs.execute('pragma table_info(%s)' % self.name)
            self.pks = [i['name'] for i in self.curs.fetchall() if i['pk']]

        elif self.db.kind == 'postgresql':
            self.curs.execute('''SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = '%s'::regclass
                    AND i.indisprimary;''' % self.name)
            self.pks = [i[0] for i in self.curs.fetchall()]


    def __repr__(self): # pragma: no cover
        return 'Table({0}, {1})'.format(self.name, self.pks)


    def __call__(self, *a, **kw):
        """
        Used to insert a row into this table.
        """
        d = Dict(self, *a, **kw)
        for ref_name in self.refs:
            d[ref_name] = None
        return d


    def __len__(self):
        return len(self.get_where())


    def get_where(self, *a, **kw):
        """
        Get all rows as Dicts where column values are as specified.  This always
        returns a generator-like object ResultsGenerator.  You can get the
        length of that generator see ResultsGenerator.count.

        If you provide only arguments, they will be paired in their respective
        order to the primary keys defined for this table.  If the primary keys
        of this table was (id,) only:

            get_where(4) is equal to get_where(id=4)

            get_where(4, 5) would raise a NoPrimaryKey error because there is
                            only one primary key.

        Primary keys are defined automatically during the init of the Table,
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
        # Need a list to replace single integers as comparisons
        operator_group = And()
        # Replace single integers with comparisons
        pk_uses = 0
        for exp in a:
            if isinstance(exp, (Comparison, Operator)):
                operator_group.append(exp)
                continue
            if not self.pks:
                raise NoPrimaryKey('No Primary Keys(s) defined for '+str(self))
            operator_group.append(self[self.pks[pk_uses]] == exp)
            pk_uses += 1
        # Add any key/values as comparisons
        for key, value in kw.items():
            operator_group.append(self[key] == value)

        order_by = None
        if self.order_by:
            order_by = self.order_by
        elif self.pks:
            order_by = str(self.pks[0])+' ASC'
        query = Select(self.name, operator_group).order_by(order_by)
        return ResultsGenerator(self, query, self.db)


    def get_one(self, *a, **kw):
        """
        Get a single row as a Dict from the Database that matches the
        arguments provided to this method.  See Table.get_where for more
        details.

        If more than one row could be returned, this will raise an
        UnexpectedRows error.
        """
        l = list(self.get_where(*a, **kw))
        if len(l) > 1:
            raise UnexpectedRows('More than one row selected.')
        return l[0]


    def count(self):
        """
        Get the count of rows in this table.
        """
        self.curs.execute('SELECT COUNT(*) FROM {table}'.format(
            table=self.name))
        return int(self.curs.fetchone()[0])


    def columns(self):
        """
        Get a list of columns of a table.
        """
        if self.db.kind == 'sqlite3':
            key = 'name'
        else:
            key = 'column_name'
        return [i[key] for i in self.columns_info()]


    def columns_info(self):
        """
        Get a dictionary that contains information about all columns of this
        table.
        """
        if self.db.kind == 'sqlite3':
            sql = "PRAGMA TABLE_INFO("+str(self.name)+")"
            self.curs.execute(sql)
            return [dict(i) for i in self.curs.fetchall()]
        else:
            sql = "SELECT * FROM information_schema.columns WHERE table_name=%s"
            self.curs.execute(sql, [self.name,])
            return [dict(i) for i in self.curs.fetchall()]


    def __setitem__(self, ref_name, ref):
        if ref.column1.table != self:
            # Dict.__getitem__ expects the columns to be in a particular order,
            # fix any order issues.
            ref.column1, ref.column2 = ref.column2, ref.column1
        self.fks[ref.column1.column] = ref_name
        self.refs[ref_name] = ref


    def __getitem__(self, ref_name):
        try:
            return self.refs[ref_name]
        except KeyError:
            if self.db.kind == 'sqlite3':
                return SqliteColumn(self, ref_name)
            else:
                return Column(self, ref_name)



class Dict(dict):
    """
    This is a represenation of a database row that behaves exactly like a
    dictionary, you may update your database row using update or simply by
    setting an item.  After you make changes, be sure to call "flush" to send
    your changes to the DB.  Your changes will not be commited or rolled-back,
    you must do that.

    This relies heavily on primary keys and they should be specified.  Really,
    your tables should have a primary key of some sort.  If not, this will
    pretty much be a read-only object.

    You can change the primary key of an instance.

    Use setitem:
    >>> d['manager_id'] = 4

    Use an update:
    >>> d.update({'manager_id':4})

    Update using another Dict:
    >>> d1.update(d2.no_pks())

    Make sure to send your changes to the database:
    >>> d.flush()

    Remove a row:
    >>> d.delete()
    """

    def __init__(self, table, *a, **kw):
        self._table = table
        self._in_db = False
        self._curs = table.db.curs
        super(Dict, self).__init__(*a, **kw)
        self._old = self.no_refs()


    def flush(self):
        """
        Insert this dictionary into it's table if its no yet in the Database,
        or Update it's row if it is already in the database.  This method
        relies heavily on the primary keys of the row's respective table.  If
        no primary keys are specified, this method will not function!

        All original column/values will bet inserted/updated by this method.
        All references will be flushed as well.
        """
        if self._table.refs:
            for ref in [i for i in self.references().values() if i]:
                ref.flush()

        if self._table.db.kind == 'sqlite3':
            insert = SqliteInsert
            update = SqliteUpdate
        else:
            # Default to Postgresql insert
            insert = Insert
            update = Update

        if not self._in_db:
            # Insert this Dict into it's respective table, interpolating
            # my values into the query
            query = insert(self._table.name, **json_dicts(self.no_refs())
                    ).returning('*')
            self._execute_query(query)
            self._in_db = True
            d = self._curs.fetchone()
        else:
            # Update this dictionary's row
            if not self._table.pks:
                raise NoPrimaryKey(
                        'Cannot update to {0}, no primary keys defined.'.format(
                    self._table))
            # Update without references, "wheres" are the primary values
            query = update(self._table.name, **json_dicts(self.no_refs())
                    ).where(self.pk_and(self._old))
            self._execute_query(query)
            d = self

        super(Dict, self).__init__(d)
        self._old = self.no_refs()
        return self


    def _execute_query(self, query):
        built = query.build()
        if isinstance(built, list):
            for sql, values in built:
                self._curs.execute(sql, values)
        else:
            sql, values = built
            self._curs.execute(sql, values)


    def pk_and(self, pk_dict=None):
        """
        Return an And() of all this Dict's primary key values. i.e.
        And(id=1, other_primary=4)
        """
        pk_dict = pk_dict or self
        return And(*[self._table[k]==v for k,v in pk_dict.items() if k in \
                self._table.pks])


    def delete(self):
        """
        Delete this row from it's table in the database.  Requires primary
        keys to be specified.
        """
        query = Delete(self._table.name).where(self.pk_and())
        self._execute_query(query)


    def no_pks(self):
        """
        Return a dictionary without the primary keys that are associated with
        this Dict in the Database.  This should be used when doing an update
        of another Dict.
        """
        return dict([(k,v) for k,v in self.items() if k not in self._table.pks])


    def no_refs(self):
        """
        Return a dictionary without the key/value(s) added by a reference.  They
        should never be sent in the query to the Database.
        """
        return dict([(k,v) for k,v in self.items() if k not in self._table.refs])


    def references(self):
        """
        Return a dictionary of only the referenced rows.
        """
        return dict([(k,v) for k,v in self.items() if k in self._table.refs])


    def __getitem__(self, key):
        """
        Get the provided "key" from this Dict instance.  If the key refers to a
        referenced row, get that row first.  Will only get a referenced row
        once, until the referenced row's foreign key is changed.
        """
        ref = self._table.refs.get(key)
        if not ref and key not in self:
            raise KeyError(str(key))
        # Only get the referenced row once, if it has a value, the reference's
        # column hasn't been changed.
        val = super(Dict, self).get(key)
        if ref and not val:
            table = ref.column2.table
            comparison = table[ref.column2.column] == self[ref.column1.column]

            if ref.many:
                gen = table.get_where(comparison)
            else:
                try:
                    gen = table.get_one(comparison)
                except IndexError:
                    # No results returned, must not be set
                    return None

            if ref._substratum and ref.many:
                gen = [i[ref._substratum] for i in gen]
            elif ref._substratum:
                gen = gen[ref._substratum]

            if not ref.many:
                # TODO Only caching one-to-one references, will need to cache
                # one-to-many
                super(Dict, self).__setitem__(key, gen)
            val = gen
        return val


    def get(self, key, default=None):
        # Provide the same functionality as a dict.get, but use this class's
        # __getitem__ instead of builtin __getitem__
        return self[key] if key in self else default


    def __setitem__(self, key, value):
        """
        Set self[key] to value.  If key is a reference's matching foreign key,
        set the reference to None.
        """
        ref = self._table.fks.get(key)
        if ref:
            super(Dict, self).__setitem__(ref, None)
        return super(Dict, self).__setitem__(key, value)


    __getitem__.__doc__ += dict.__getitem__.__doc__
    get.__doc__ = dict.get.__doc__



