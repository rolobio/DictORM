import asyncio
from contextlib import contextmanager
from itertools import chain

import dictorm
from .asyncpg import Insert, Select, Update, Column, Delete, And


class DictDB(dictorm.DictDB):

    def __init__(self, db_conn):
        self.conn = db_conn
        self.kind = 'asyncpg'
        self.insert = Insert
        self.update = Update
        self.column = Column
        self.select = Select
        self.delete = Delete

    async def init(self):
        self.curs = await self.get_cursor()
        await self.refresh_tables()

    async def get_cursor(self):
        return self.conn

    async def __list_tables(self):
        return await self.curs.fetch('''SELECT DISTINCT table_name
                FROM information_schema.columns
                WHERE table_schema='public' ''')

    async def table_factory(self):
        return Table

    async def refresh_tables(self):
        """
        Create all Table instances from all tables found in the database.
        """
        if self.keys():
            # Reset this DictDB because it contains old tables
            super(DictDB, self).__init__()
        table_cls = await self.table_factory()
        for table in await self.__list_tables():
            self[table['table_name']] = table_cls(table['table_name'], self)
            await self[table['table_name']].init()


class ResultsGenerator(dictorm.ResultsGenerator):

    def __init__(self, table, query, db):
        self.table = table
        self.query = query
        self.cache = []
        self.completed = False
        self.executed = False
        self.db_kind = db.kind
        self.db = db
        self.curs = None
        self._nocache = False

    async def __aenter__(self):
        sql, values = self.query.build()
        async with self.db.conn.transaction():
            self.curs = await self.db.conn.cursor(sql, *values)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.curs.fetchrow()


_json_column_types = ('json', 'jsonb')


class Table(dictorm.Table):

    def __init__(self, table_name, db):
        self.name = table_name
        self.db = db
        self.curs = db.curs
        self.pks = []
        self.refs = {}
        self.order_by = None
        self.fks = {}
        self.cached_columns_info = None
        self.cached_column_names = None

    async def _refresh_pks(self):
        """
        Get a list of Primary Keys set for this table in the DB.
        """
        query = '''SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '{}'::regclass
                AND i.indisprimary;'''.format(self.name)
        self.pks = [i[0] for i in await self.curs.fetch(query)]

    @classmethod
    def _results_generator_factory(cls):
        return ResultsGenerator

    async def init(self):
        # Detect json column types for this table's columns
        data_types = [i['data_type'].lower() for i in await self.columns_info]
        self.has_json = True if \
            [i for i in _json_column_types if i in data_types] \
            else False
        await self._refresh_pks()

    @property
    async def columns_info(self):
        """
        Get a dictionary that contains information about all columns of this
        table.
        """
        if self.cached_columns_info:
            return self.cached_columns_info

        sql = "SELECT * FROM information_schema.columns WHERE table_name=$1"
        self.cached_columns_info = [dict(i) for i in await self.curs.fetch(sql, self.name)]
        return self.cached_columns_info

    @property
    async def column_names(self):
        if not self.cached_column_names:
            self.cached_column_names = set(i['column_name'] for i in await self.columns_info)
        return self.cached_column_names

    @classmethod
    def _dict_factory(cls):
        return Dict

    async def get_where(self, *a, **kw):
        # All args/kwargs are combined in an SQL And comparison
        operator_group = dictorm.args_to_comp(And(), self, *a, **kw)

        order_by = None
        if self.order_by:
            order_by = self.order_by
        elif self.pks:
            order_by = str(self.pks[0]) + ' ASC'
        query = Select(self.name, operator_group).order_by(order_by)
        sql, values = query.build()
        return map(self, await self.db.curs.fetch(sql, *values))

    async def get_one(self, *a, **kw):
        """
        Get a single row as a Dict from the Database that matches the arguments
        provided to this method.  See Table.get_where for more details.

        If more than one row could be returned, this will raise an
        UnexpectedRows error.
        """
        rgen = await self.get_where(*a, **kw)
        try:
            i = next(rgen)
        except StopIteration:
            return None
        try:
            next(rgen)
        except StopIteration:  # Should only be one result
            pass
        else:
            raise dictorm.UnexpectedRows('More than one row selected.')
        return i


class Dict(dictorm.Dict):

    async def flush(self):
        items = self._get_db_items()
        items = {k: (await v if asyncio.iscoroutine(v) else v)
                 for k, v in items.items() if k in await self._table.column_names}
        if not self._in_db:
            query = self._table.db.insert(self._table.name, **items).returning('*')
            d = await self.__execute_query(query)
            self._in_db = True
        else:
            if not self._table.pks:
                raise dictorm.NoPrimaryKey(
                    'Cannot update to {0}, no primary keys defined.'.format(
                        self._table))
            query = self._table.db.update(self._table.name, **items
                                          ).where(self._old_pk_and or self.pk_and()).returning('*')
            d = await self.__execute_query(query)

        if d:
            super(dictorm.Dict, self).__init__(d)
        self._old_pk_and = self.pk_and()
        return self

    async def __execute_query(self, query):
        sql, values = await query.build()
        return await self._curs.fetchrow(sql, *values)

    @classmethod
    def _results_generator_factory(cls):
        return ResultsGenerator

    async def get(self, key, default=None):
        # Provide the same functionality as a dict.get, but use this class's
        # getitem instead of dictorm.Dict.getitem
        return await self[key] if key in self else default

    async def _aget(self, key):
        ref = self._table.refs.get(key)
        if not ref and key not in self:
            raise KeyError(str(key))
        # Only get the referenced row once, if it has a value, the reference's
        # column hasn't been changed.
        val = super(dictorm.Dict, self).get(key)
        if ref and not val:
            table = ref.column2.table
            comparison = table[ref.column2.column] == await self[ref.column1.column]

            if ref.many:
                gen = await table.get_where(comparison)
                if ref._substratum:
                    gen = [await i[ref._substratum] for i in gen]
                if ref._aggregate:
                    gen = list(chain(*gen))
                return gen
            else:
                val = await table.get_one(comparison)
                if ref._substratum and val:
                    return await val[ref._substratum]
                super(Dict, self).__setitem__(key, val)
        return val

    def __getitem__(self, item):
        return self._aget(item)
