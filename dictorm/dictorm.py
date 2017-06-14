"""What if you could insert a Python dictionary into the database?  DictORM allows you to select/insert/update rows of a database as if they were Python Dictionaries."""
__version__ = '3.5'

from copy import deepcopy
from itertools import chain
from json import dumps
from sys import modules

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
    raise ImportError('Failed to import psycopg2 or sqlite3.  These are the'
            'only supported Databases and you must import one of them')


class NoPrimaryKey(Exception): pass
class UnexpectedRows(Exception): pass
class NoCache(Exception): pass


global _json_dicts
def _json_dicts(d):
    """
    Convert all dictionaries contained in this object into JSON strings.
    """
    for key, value in d.items():
        if isinstance(value, dict):
            d[key] = dumps(value)
    return d


def set_json_dicts(func):
    "Used only for testing"
    global _json_dicts
    original, _json_dicts = _json_dicts, func
    return original


class DictDB(dict):
    """
    Get all the tables from the provided Psycopg2/Sqlite3 connection.  Create a
    Table instance for each table, and keep them in this DictDB using the
    table's name as a key.

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
            self.insert = SqliteInsert
            self.update = SqliteUpdate
            self.column = SqliteColumn
        else:
            self.kind = 'postgresql'
            self.insert = Insert
            self.update = Update
            self.column = Column
        self.select = Select
        self.delete = Delete

        self.curs = self.get_cursor()
        self.refresh_tables()
        self.conn.rollback()
        super(DictDB, self).__init__()


    @classmethod
    def table_factory(cls):
        return Table


    def __list_tables(self):
        if self.kind == 'sqlite3':
            self.curs.execute('SELECT name FROM sqlite_master WHERE type ='
                    '"table"')
        else:
            self.curs.execute('''SELECT DISTINCT table_name
                    FROM information_schema.columns
                    WHERE table_schema='public' ''')
        return self.curs.fetchall()


    def get_cursor(self):
        """
        Returns a cursor from the provided database connection that DictORM
        objects expect.
        """
        if self.kind == 'sqlite3':
            self.conn.row_factory = sqlite3.Row
            return self.conn.cursor()
        elif self.kind == 'postgresql':
            return self.conn.cursor(cursor_factory=DictCursor)


    def refresh_tables(self):
        """
        Create all Table instances from all tables found in the database.
        """
        if self.keys():
            # Reset this DictDB because it contains old tables
            super(DictDB, self).__init__()
        table_cls = self.table_factory()
        for table in self.__list_tables():
            if self.kind == 'sqlite3':
                self[table['name']] = table_cls(table['name'], self)
            else:
                self[table['table_name']] = table_cls(table['table_name'], self)



def args_to_comp(operator, table, *args, **kwargs):
    """
    Add arguments to the provided operator paired with their respective primary
    key.
    """
    operator = operator or And()
    pk_uses = 0
    pks = table.pks
    for val in args:
        if isinstance(val, (Comparison, Operator)):
            # Already a Comparison/Operator, just add it
            operator += (val,)
            continue
        if not table.pks:
            raise NoPrimaryKey('No Primary Keys(s) defined for '+str(table))
        try:
            # Create a Comparison using the next Primary Key
            operator += (table[pks[pk_uses]] == val,)
        except IndexError:
            raise NoPrimaryKey('Not enough Primary Keys(s) defined for '+
                    str(table))
        pk_uses += 1

    for k,v in kwargs.items():
        operator += table[k] == v

    return operator



class ResultsGenerator:
    """
    This class replicates a Generator, the query will not be executed and no
    results will be fetched until "__next__" is called.  Results are cached and
    will not be gotten again.  To get new results if they have been changed,
    create a new ResultsGenerator instance, or flush your Dict.
    """

    def __init__(self, table, query, db):
        self.table = table
        self.query = query
        self.cache = []
        self.completed = False
        self.executed = False
        self.db_kind = db.kind
        self.db = db
        self.curs = self.db.get_cursor()
        self._nocache = False


    def __iter__(self):
        if self.completed:
            return iter(self.cache)
        else:
            return self


    def __next__(self):
        self.__execute_once()
        d = self.curs.fetchone()
        if not d:
            self.completed = True
            raise StopIteration
        # Convert returned dictionary to a Dict
        d = self.table(d)
        d._in_db = True
        if self._nocache == False:
            self.cache.append(d)
        return d


    def __execute_once(self):
        if not self.executed:
            self.executed = True
            sql, values = self.query.build()
            self.curs.execute(sql, values)


    # for python 2.7
    next = __next__


    def __len__(self):
        self.__execute_once()
        if self.db_kind == 'sqlite3':
            # sqlite3's cursor.rowcount doesn't support select statements
            # returns a 0 because this method is called when a ResultsGenerator
            # is converted into a list()
            return 0
        return self.curs.rowcount


    def __getitem__(self, i):
        if isinstance(i, int) and i >= 0:
            try:
                return self.cache[i]
            except IndexError:
                # Haven't gotten that far yet, get the rest
                pass
            while i >= 0 and i <= len(self.cache):
                try:
                    return next(self)
                except StopIteration:
                    if self._nocache == True:
                        raise NoCache('Caching has been disabled.')
                    else:
                        raise IndexError('No row of that index')

        if not self.completed:
            # Get all rows
            list(self)
        return self.cache[i]


    def nocache(self):
        """
        Return a new ResultsGenerator that will not cache the results.
        """
        query = deepcopy(self.query)
        results = ResultsGenerator(self.table, query, self.db)
        results._nocache = True
        return results


    def refine(self, *a, **kw):
        """
        Return a new ResultsGenerator with a refined query.  Arguments provided
        are expected to be Operators/Comparisons.  Keyword Arguments are
        converted into == Comparisons.

        Arguments:
            .refine(Person['name']=='steve', Person['foo']=='bar')

        Keyword Arguments:
            .refine(name='steve', foo='bar') # Same refinement as the above
                                             # example
        """
        query = deepcopy(self.query)
        query = args_to_comp(query, self.table, *a, **kw)
        return ResultsGenerator(self.table, query, self.db)


    def order_by(self, order_by):
        """
        Return a new ResultsGenerator with a modified ORDER BY clause.  Expects
        a raw SQL string.

        Examples:
            .order_by('id ASC')
            .order_by('entrydate DESC')
        """
        query = deepcopy(self.query).order_by(order_by)
        return ResultsGenerator(self.table, query, self.db)


    def limit(self, limit):
        """
        Return a new ResultsGenerator with a modified LIMIT clause.  Expects a
        raw SQL string.

        Examples:
            .limit(10)
            .limit('ALL')
        """
        query = deepcopy(self.query).limit(limit)
        return ResultsGenerator(self.table, query, self.db)


    def offset(self, offset):
        """
        Return a new ResultsGenerator with a modified OFFSET clause.  Expects a
        raw SQL string.

        Example:
            .offset(10)
        """
        query = deepcopy(self.query).offset(offset)
        return ResultsGenerator(self.table, query, self.db)



_json_column_types = ('json', 'jsonb')

class Table(object):
    """
    A representation of a DB table.  You will primarily retrieve rows (Dicts)
    from the database using the get_where and get_one methods.

    Insert into this table:

    >>> your_table(some_column='some value', other=False)
    {'some_column':'some value', 'other':False}

    Get all rows that need to be updated:

    >>> list(table.get_where(outdated=True))
    [Dict(), Dict(), Dict(), Dict()]

    Get a single row (will raise an UnexpectedRow error if more than one row
    could have been returned):

    >>> table.get_one(id=12)
    Dict()
    >>> table.get_one(manager_id=14)
    Dict()
    >>> table.get_one(id=500) # id does not exist
    None

    You can reference another table using setitem.  Link to an employee's
    manager using the manager's id, and the employee's manager_id.

    >>> Person['manager'] = Person['manager_id'] == Person['id']
    >>> bob['manager']
    Dict()

    The foreign key should be on the right side of the Comparison.
    >>> Person['manager'] = Person['manager_id'] == Person['id'] # right
    >>> Person['manager'] = Person['id'] == Person['manager_id'] # wrong

    Reference a manager's subordinates using their collective manager_id's.
    Again, the foreign key is on the right.

    >>> Person['subordinates'] = Person['id'].many(Person['manager_id'])
    >>> list(bob['manager'])
    [Dict(), Dict()]

    Table.get_where returns a generator object, this makes it so you won't have
    an entire table's object in memory at once, they are generated when gotten:

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
        self._refresh_pks()
        self.order_by = None
        self.fks = {}
        self.cached_columns_info = None
        # Detect json column types for this table's columns
        type_column_name = 'type' if db.kind == 'sqlite3' else 'data_type'
        data_types = [i[type_column_name].lower() for i in self.columns_info]
        self.has_json = True if \
                [i for i in _json_column_types if i in data_types]\
                else False


    def _refresh_pks(self):
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


    def get_where(self, *a, **kw):
        """
        Get all rows as Dicts where column values are as specified.  This always
        returns a generator-like object ResultsGenerator.

        If you provide only arguments, they will be paired in their respective
        order to the primary keys defined or this table.  If the primary keys
        of this table was ('id',) only:

            get_where(4) is equal to get_where(id=4)

            get_where(4, 5) would raise a NoPrimaryKey error because there is
                            only one primary key.

        Primary keys are defined automatically during the init of the Table, but
        you can overwrite that by changing .pks:

        >>> your_table.pks = ['id', 'some_column', 'whatever_you_want']

            get_where(4, 5, 6) is now equal to get_where(id=4, some_column=5,
                                                    whatever_you_want=6)

        If there were two primary keys, such as in a join table (id, group):

            get_where(4, 5) is equal to get_where(id=4, group=5)

        You cannot use this method without primary keys, unless you specify the
        column you are matching.

        >>> get_where(some_column=83)
        ResultsGenerator()

        >>> get_where(4) # no primary keys defined!
        NoPrimaryKey()

        """
        # All args/kwargs are combined in an SQL And comparison
        operator_group = args_to_comp(And(), self, *a, **kw)

        order_by = None
        if self.order_by:
            order_by = self.order_by
        elif self.pks:
            order_by = str(self.pks[0])+' ASC'
        query = Select(self.name, operator_group).order_by(order_by)
        return ResultsGenerator(self, query, self.db)


    def get_one(self, *a, **kw):
        """
        Get a single row as a Dict from the Database that matches the arguments
        provided to this method.  See Table.get_where for more details.

        If more than one row could be returned, this will raise an
        UnexpectedRows error.
        """
        rgen = self.get_where(*a, **kw)
        try:
            i = next(rgen)
        except StopIteration:
            return None
        try:
            next(rgen)
            raise UnexpectedRows('More than one row selected.')
        except StopIteration: # Should only be one result
            pass
        return i


    def count(self):
        """
        Get the count of rows in this table.
        """
        self.curs.execute('SELECT COUNT(*) FROM {table}'.format(
            table=self.name))
        return int(self.curs.fetchone()[0])


    @property
    def columns(self):
        """
        Get a list of columns of a table.
        """
        if self.db.kind == 'sqlite3':
            key = 'name'
        else:
            key = 'column_name'
        return [i[key] for i in self.columns_info]


    @property
    def columns_info(self):
        """
        Get a dictionary that contains information about all columns of this
        table.
        """
        if self.cached_columns_info:
            return self.cached_columns_info

        if self.db.kind == 'sqlite3':
            sql = "PRAGMA TABLE_INFO("+str(self.name)+")"
            self.curs.execute(sql)
            self.cached_columns_info = [dict(i) for i in self.curs.fetchall()]
        else:
            sql = "SELECT * FROM information_schema.columns WHERE table_name=%s"
            self.curs.execute(sql, [self.name,])
            self.cached_columns_info = [dict(i) for i in self.curs.fetchall()]
        return self.cached_columns_info


    def __setitem__(self, ref_name, ref):
        """
        Create reference that will be gotten by all Dicts created from this
        table.

        Example:
            Person['manager'] = Person['manager_id'] == Person['id']

        For more examples see Table's doc.
        """
        if ref.column1.table != self:
            # Dict.__getitem__ expects the columns to be in a particular order,
            # fix any order issues.
            ref.column1, ref.column2 = ref.column2, ref.column1
        self.fks[ref.column1.column] = ref_name
        self.refs[ref_name] = ref


    def __getitem__(self, ref_name):
        """
        Get a reference if it has already been created.  Otherwise, return a
        Column object which is used to create a reference.
        """
        if ref_name in self.refs:
            return self.refs[ref_name]
        return self.db.column(self, ref_name)



class Dict(dict):
    """
    This is a represenation of a database row that behaves exactly like a
    dictionary.  You may update this dictionary using update or simply by
    setting an item.  After you make changes, be sure to call "flush" to send
    your changes to the DB.  Your changes will not be commited or rolled-back,
    you must do that.

    This requires primary keys and they should be specified.  Really, your
    tables should have a primary key of some sort.  If not, this will pretty
    much be a read-only object.

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
        self._old_pk_and = None


    def flush(self):
        """
        Insert this dictionary into it's table if its no yet in the Database, or
        Update it's row if it is already in the database.  This method relies
        heavily on the primary keys of the row's respective table.  If no
        primary keys are specified, this method will not function!

        All original column/values will bet inserted/updated by this method.
        All references will be flushed as well.
        """
        if self._table.refs:
            for i in self.values():
                if isinstance(i, Dict):
                    i.flush()

        # This will be sent to the DB, don't convert dicts to json unless
        # the table has json columns.
        items = self.no_refs()
        if self._table.has_json:
            items = _json_dicts(items)

        if not self._in_db:
            # Insert this Dict into it's respective table, interpolating
            # my values into the query
            query = self._table.db.insert(self._table.name, **items
                    ).returning('*')
            self.__execute_query(query)
            self._in_db = True
            d = self._curs.fetchone()
        else:
            # Update this dictionary's row
            if not self._table.pks:
                raise NoPrimaryKey(
                        'Cannot update to {0}, no primary keys defined.'.format(
                    self._table))
            # Update without references, "wheres" are the primary values
            query = self._table.db.update(self._table.name, **items
                    ).where(self._old_pk_and or self.pk_and())
            self.__execute_query(query)
            d = self

        super(Dict, self).__init__(d)
        self._old_pk_and = self.pk_and()
        return self


    def delete(self):
        """
        Delete this row from it's table in the database.  Requires primary keys
        to be specified.
        """
        query = self._table.db.delete(self._table.name).where(
                self._old_pk_and or self.pk_and())
        self.__execute_query(query)


    def __execute_query(self, query):
        built = query.build()
        if isinstance(built, list):
            for sql, values in built:
                self._curs.execute(sql, values)
        else:
            sql, values = built
            self._curs.execute(sql, values)


    def pk_and(self):
        """
        Return an And() of all this Dict's primary key and values. i.e.
        And(id=1, other_primary=4)
        """
        return And(*[self._table[k]==v for k,v in self.items() if k in \
                self._table.pks])


    def no_pks(self):
        """
        Return a dictionary without the primary keys that are associated with
        this Dict in the Database.  This should be used when doing an update of
        another Dict.
        """
        return dict([(k,v) for k,v in self.items() if k not in self._table.pks])


    def no_refs(self):
        """
        Return a dictionary without the key/value(s) added by a reference.  They
        should never be sent in the query to the Database.
        """
        return dict([(k,v) for k,v in self.items() if k not in self._table.refs]
                )


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
                if ref._substratum:
                    gen = [i[ref._substratum] for i in gen]
                if ref._aggregate:
                    gen = list(chain(*gen))
                return gen
            else:
                val = table.get_one(comparison)
                if ref._substratum and val:
                    return val[ref._substratum]
                super(Dict, self).__setitem__(key, val)
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


    # Copy docs for methods that recreate dict() functionality
    __getitem__.__doc__ += dict.__getitem__.__doc__
    get.__doc__ = dict.get.__doc__



