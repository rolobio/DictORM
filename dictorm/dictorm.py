"""What if you could insert a Python dictionary into the database?  DictORM allows you to select/insert/update rows of a database as if they were Python Dictionaries."""
import enum
import sqlite3
from json import dumps
from typing import Union, Optional, List

__version__ = '4.2'

from contextlib import contextmanager
from itertools import chain
from sys import modules

from .pg import Select, Insert, Update, Delete
from .pg import And, QueryHint
from .pg import Column, Comparison, Operator
from .sqlite import Insert as SqliteInsert
from .sqlite import Column as SqliteColumn
from .sqlite import Update as SqliteUpdate

db_conn_type = sqlite3.Connection
CursorHint = sqlite3.Cursor
sqlite3.register_adapter(dict, dumps)

try:  # pragma: no cover
    from psycopg2.extras import _connection
    from psycopg2.extras import DictCursor, Json
    from psycopg2.extensions import register_adapter

    db_conn_type = Union[db_conn_type, _connection]

    register_adapter(dict, Json)

    CursorHint = Union[CursorHint, DictCursor]
except ImportError:  # pragma: no cover
    pass

__all__ = [
    '__version__',
    'And',
    'CannotUpdateColumn',
    'DBKind',
    'Dict',
    'DictDB',
    'NoCache',
    'NoPrimaryKey',
    'RawQuery',
    'ResultsGenerator',
    'Table',
    'UnexpectedRows',
]


class NoPrimaryKey(Exception):
    pass


class UnexpectedRows(Exception):
    pass


class NoCache(Exception):
    pass


class CannotUpdateColumn(Exception):
    pass


class DBKind(enum.Enum):
    postgres = enum.auto()
    sqlite3 = enum.auto()


class Dict(dict):
    """
    This is a representation of a database row that behaves exactly like a
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
        self.table: Table = table
        self._in_db = False
        self._curs: CursorHint = table.db.curs
        super(Dict, self).__init__(*a, **kw)
        self._old_pk_and = None

    def flush(self):
        """
        Insert this dictionary into it's table if its not yet in the Database, or
        Update it's row if it is already in the database.  This method relies
        heavily on the primary keys of the row's respective table.  If no
        primary keys are specified, this method will not function!

        All original column/values will bet inserted/updated by this method.
        All references will be flushed as well.
        """
        if self.table.refs:
            for i in self.values():
                if isinstance(i, Dict):
                    i.flush()

        # This will be sent to the DB, don't convert dicts to json unless
        # the table has json columns.
        items = self.no_refs()

        # Insert/Update only with columns present on the table, this allows custom
        # instances of Dicts to be inserted even if they have columns not on the table
        items = {k: v for k, v in items.items() if k in self.table.updateable_column_names}

        if not self._in_db:
            # Insert this Dict into it's respective table, interpolating
            # my values into the query
            query = self.table.db.insert(self.table.name, **items
                                         ).returning('*')
            d = self.__execute_query(query)
            self._in_db = True
        else:
            # Update this dictionary's row
            if not self.table.pks:
                raise NoPrimaryKey(
                    'Cannot update to {0}, no primary keys defined.'.format(
                        self.table))
            # Update without references, "wheres" are the primary values
            query = self.table.db.update(self.table.name, **items
                                         ).where(self._old_pk_and or self.pk_and()).returning('*')
            d = self.__execute_query(query)

        if d:
            super(Dict, self).__init__(d)
        self._old_pk_and = self.pk_and()
        return self

    def delete(self):
        """
        Delete this row from it's table in the database.  Requires primary keys
        to be specified.
        """
        query = self.table.db.delete(self.table.name).where(
            self._old_pk_and or self.pk_and())
        return self.__execute_query(query)

    def __execute_query(self, query):
        built = query.build()
        if isinstance(built, list):
            for sql, values in built:
                self._curs.execute(sql, values)
            if query.append_returning:
                return self._curs.fetchone()
        else:
            sql, values = built
            self._curs.execute(sql, values)
            if query._returning:
                return self._curs.fetchone()

    def pk_and(self):
        """
        Return an And() of all this Dict's primary key and values. i.e.
        And(id=1, other_primary=4)
        """
        return And(*[self.table[k] == v for k, v in self.items() if k in \
                     self.table.pks])

    def no_pks(self):
        """
        Return a dictionary without the primary keys that are associated with
        this Dict in the Database.  This should be used when doing an update of
        another Dict.
        """
        return {k: v for k, v in self.items() if k not in self.table.pks}

    def no_refs(self):
        """
        Return a dictionary without the key/value(s) added by a reference.  They
        should never be sent in the query to the Database.
        """
        return {k: v for k, v in self.items() if k not in self.table.refs}

    def references(self):
        """
        Return a dictionary of only the referenced rows.
        """
        return {k: v for k, v in self.items() if k in self.table.refs}

    def __getitem__(self, key):
        """
        Get the provided "key" from this Dict instance.  If the key refers to a
        referenced row, get that row first.  Will only get a referenced row
        once, until the referenced row's foreign key is changed.
        """
        ref = self.table.refs.get(key)
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
        ref = self.table.fks.get(key)
        if ref:
            super(Dict, self).__setitem__(ref, None)
        if key not in self.table.updateable_column_names:
            raise CannotUpdateColumn(
                f'Column "{key}" cannot be updated, it may not exist or it may be a special column.')
        return super(Dict, self).__setitem__(key, value)

    # Copy docs for methods that recreate dict() functionality
    __getitem__.__doc__ += dict.__getitem__.__doc__
    get.__doc__ = dict.get.__doc__


class RawQuery:
    """
    Used only for Table.get_raw.  Merely returns the provided args when build is called.
    """

    def __init__(self, sql_query, *args):
        self.sql_query = sql_query
        self.args = args

    def build(self):
        return self.sql_query, self.args


class ResultsGenerator:
    """
    This class replicates a Generator, the query will not be executed and no
    results will be fetched until "__next__" is called.  Results are cached and
    will not be gotten again.  To get new results if they have been changed,
    create a new ResultsGenerator instance, or flush your Dict.
    """

    def __init__(self, table, query: QueryHint, db):
        self.table: Table = table
        self.query = query
        self.cache = []
        self.completed = False
        self.executed = False
        self.db_kind = db.kind
        self.db: DictDB = db
        self.curs: CursorHint = self.db.get_cursor()
        self._nocache = False

    def __iter__(self):
        if self.completed:
            return iter(self.cache)
        else:
            return self

    def __next__(self) -> Dict:
        self.__execute_once()
        d = self.curs.fetchone()
        if not d:
            self.completed = True
            raise StopIteration
        # Convert returned dictionary to a Dict
        d = self.table(d)
        d._in_db = True
        if self._nocache is False:
            self.cache.append(d)
        return d

    def __execute_once(self):
        if not self.executed:
            self.executed = True
            sql, values = self.query.build()
            self.curs.execute(sql, values)

    def __len__(self) -> int:
        self.__execute_once()
        if self.db_kind == DBKind.sqlite3:
            # sqlite3's cursor.rowcount doesn't support select statements
            # returns a 0 because this method is called when a ResultsGenerator
            # is converted into a list()
            return 0
        return self.curs.rowcount

    def __getitem__(self, i) -> Dict:
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
        results = ResultsGenerator(self.table, self.query._copy(), self.db)
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
        query = self.query._copy()
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
        query = self.query._copy().order_by(order_by)
        return ResultsGenerator(self.table, query, self.db)

    def limit(self, limit):
        """
        Return a new ResultsGenerator with a modified LIMIT clause.  Expects a
        raw SQL string.

        Examples:
            .limit(10)
            .limit('ALL')
        """
        query = self.query._copy().limit(limit)
        return ResultsGenerator(self.table, query, self.db)

    def offset(self, offset):
        """
        Return a new ResultsGenerator with a modified OFFSET clause.  Expects a
        raw SQL string.

        Example:
            .offset(10)
        """
        query = self.query._copy().offset(offset)
        return ResultsGenerator(self.table, query, self.db)


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

    >>> Person = db['person']
    >>> Person['manager'] = Person['manager_id'] == Person['id']
    >>> bob = Person(name='Bob')
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
        self._updateable_column_names = set()
        self.cached_columns_info = None
        self.cached_column_names = None

    def _refresh_pks(self):
        """
        Get a list of Primary Keys set for this table in the DB.
        """
        if self.db.kind == DBKind.sqlite3:
            self.curs.execute('pragma table_info(%s)' % self.name)
            self.pks = [i['name'] for i in self.curs.fetchall() if i['pk']]

        elif self.db.kind == DBKind.postgres:
            self.curs.execute('''SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = '%s'::regclass
                    AND i.indisprimary;''' % self.name)
            self.pks = [i[0] for i in self.curs.fetchall()]

    def __repr__(self) -> str:  # pragma: no cover
        return f'Table({self.name}, {self.pks})'

    def __call__(self, *a, **kw) -> Dict:
        """
        Used to insert a row into this table.
        """
        d = Dict(self, *a, **kw)
        for ref_name in self.refs:
            d[ref_name] = None
        return d

    def get_where(self, *a, **kw) -> ResultsGenerator:
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

        >>> your_table.get_where(some_column=83)
        ResultsGenerator()

        >>> your_table.get_where(4) # no primary keys defined!
        NoPrimaryKey()

        Check if a Dict belongs to this Table:
        >>> bob in Person
        True

        """
        # When column names are quoted in an SQLite statement and the column doesn't exist, SQLite doesn't raise
        # an exception.  We'll raise an exception if any columns don't exist.
        if self.db.kind == DBKind.sqlite3:
            bad_columns = set(kw.keys()).difference(self.column_names)
            if bad_columns:
                raise sqlite3.OperationalError(f'no such column: {bad_columns.pop()}')

        # All args/kwargs are combined in an SQL And comparison
        operator_group = args_to_comp(And(), self, *a, **kw)

        order_by = None
        if self.order_by:
            order_by = self.order_by
        elif self.pks:
            order_by = str(self.pks[0]) + ' ASC'
        query = Select(self.name, operator_group).order_by(order_by)
        return ResultsGenerator(self, query, self.db)

    def get_one(self, *a, **kw) -> Optional[Dict]:
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
        except StopIteration:  # Should only be one result
            pass
        else:
            raise UnexpectedRows('More than one row selected.')
        return i

    def get_raw(self, sql_query: str, *a) -> ResultsGenerator:
        """
        Get all rows returned by the raw SQL query provided, as Dicts.  Expects
        that the query will only return columns from this instance's table.

        Extra arguments and keyword arguments pare passed to the query builder as variables.
        """
        query = RawQuery(sql_query, *a)
        return ResultsGenerator(self, query, self.db)

    def count(self) -> int:
        """
        Get the count of rows in this table.
        """
        self.curs.execute('SELECT COUNT(*) FROM {table}'.format(
            table=self.name))
        return int(self.curs.fetchone()[0])

    @property
    def columns(self) -> List[str]:
        """
        Get a list of columns of a table.
        """
        if self.db.kind == DBKind.sqlite3:
            key = 'name'
        else:
            key = 'column_name'
        return [i[key] for i in self.columns_info]

    @property
    def columns_info(self) -> List[dict]:
        """
        Get a dictionary that contains information about all columns of this
        table.
        """
        if self.cached_columns_info:
            return self.cached_columns_info

        if self.db.kind == DBKind.sqlite3:
            sql = "PRAGMA TABLE_INFO(" + str(self.name) + ")"
            self.curs.execute(sql)
            self.cached_columns_info = [dict(i) for i in self.curs.fetchall()]
        else:
            sql = "SELECT * FROM information_schema.columns WHERE table_name=%s"
            self.curs.execute(sql, [self.name, ])
            self.cached_columns_info = [dict(i) for i in self.curs.fetchall()]
        return self.cached_columns_info

    @property
    def column_names(self) -> set:
        if not self.cached_column_names:  # pragma: no cover
            if self.db.kind == DBKind.sqlite3:
                self.cached_column_names = set(i['name'] for i in
                                               self.columns_info)
            else:
                self.cached_column_names = set(i['column_name'] for i in
                                               self.columns_info)
        return self.cached_column_names

    @property
    def updateable_column_names(self) -> set:
        if self._updateable_column_names:
            return self._updateable_column_names
        # These are generated columns, the can't be updated
        if self.db.kind == DBKind.sqlite3:
            column_key = 'name'
        else:
            column_key = 'column_name'

        self._updateable_column_names = set()

        # Postgres generated columns
        self._updateable_column_names |= {i[column_key] for i in self.columns_info if
                                          not i.get('generation_expression')}
        # Referenced columns
        self._updateable_column_names |= self.refs.keys()

        return self._updateable_column_names

    def __setitem__(self, ref_name: str, ref):
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
        self._updateable_column_names = None

    def __getitem__(self, ref_name: str) -> Union[Column, SqliteColumn]:
        """
        Get a reference if it has already been created.  Otherwise, return a
        Column object which is used to create a reference.
        """
        if ref_name in self.refs:
            return self.refs[ref_name]
        return self.db.column(self, ref_name)

    def __contains__(self, item: Dict):
        """
        Compare a row's table to myself.  If the tables match, the row is a member of this
        table.

        Example:
            >>> bob in Person
            True
            >>> bob in Car
            False
        """
        if isinstance(item, Dict):
            return item.table == self
        raise ValueError('Cannot check if item is in this Table because it is not a Dict.')


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

    def __init__(self, db_conn: db_conn_type):
        self._real_getitem = super().__getitem__
        self.conn = db_conn
        if 'sqlite3' in modules and isinstance(db_conn, sqlite3.Connection):
            self.kind = DBKind.sqlite3
            self.insert = SqliteInsert
            self.update = SqliteUpdate
            self.column = SqliteColumn
        else:
            self.kind = DBKind.postgres
            self.insert = Insert
            self.update = Update
            self.column = Column
        self.select = Select
        self.delete = Delete

        self.curs = self.get_cursor()
        self.refresh_tables()
        self.conn.rollback()
        super(DictDB, self).__init__()

    def __repr__(self):  # pragma: no cover
        return f'DictDB({self.kind}, {self.conn})'

    def __getitem__(self, item: str) -> Table:
        return self._real_getitem(item)

    @classmethod
    def table_factory(cls) -> Table:
        return Table

    def __list_tables(self):
        if self.kind == DBKind.sqlite3:
            self.curs.execute('SELECT name FROM sqlite_master WHERE type ='
                              '"table"')
        else:
            self.curs.execute('''SELECT DISTINCT table_name
                    FROM information_schema.columns
                    WHERE table_schema='public' ''')
        return self.curs.fetchall()

    def get_cursor(self) -> CursorHint:
        """
        Returns a cursor from the provided database connection that DictORM
        objects expect.
        """
        if self.kind == DBKind.sqlite3:
            self.conn.row_factory = sqlite3.Row
            curs = self.conn.cursor()
            return curs
        elif self.kind == DBKind.postgres:
            curs = self.conn.cursor(cursor_factory=DictCursor)
            return curs

    def refresh_tables(self):
        """
        Create all Table instances from all tables found in the database.
        """
        if self.keys():
            # Reset this DictDB because it contains old tables
            super(DictDB, self).__init__()
        table_cls = self.table_factory()
        name_key = 'name' if self.kind == DBKind.sqlite3 else 'table_name'
        for table in self.__list_tables():
            name = table[name_key]
            self[name] = table_cls(name, self)

    @contextmanager
    def transaction(self, commit: bool = False):
        """
        Context manager to rollback changes in case of an error.

        :param commit: Commit changes on close, if True.
        :return:
        """
        try:
            yield
        except Exception:
            self.conn.rollback()
            raise
        else:
            # Commit if no exceptions occur
            if commit:
                self.conn.commit()


def args_to_comp(operator: Operator, table: Table, *args, **kwargs):
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
            raise NoPrimaryKey('No Primary Keys(s) defined for ' + str(table))
        try:
            # Create a Comparison using the next Primary Key
            operator += (table[pks[pk_uses]] == val,)
        except IndexError:
            raise NoPrimaryKey('Not enough Primary Keys(s) defined for ' +
                               str(table))
        pk_uses += 1

    for k, v in kwargs.items():
        operator += table[k] == v

    return operator
