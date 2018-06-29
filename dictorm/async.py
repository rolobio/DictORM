import asyncio

import dictorm
from dictorm.asyncpg import Column, Delete, Insert, Select, Update


class DBContext:

    def __init__(self, pool):
        self.pool = pool
        self.conn = None
        self.curs = None

    async def __aenter__(self):
        self.conn = await self.pool.acquire()
        self.curs = await self.conn.cursor()
        return self.conn, self.curs

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.curs.close()
        self.conn.close()


class DictDB(dictorm.DictDB):

    def __init__(self, pool):
        self.pool = pool
        self.kind = 'psycopg2'
        self.column = Column
        self.delete = Delete
        self.insert = Insert
        self.select = Select
        self.update = Update

        super(dict, self).__init__()

    async def init(self):
        await self.refresh_tables()
        return self

    def table_factory(self):
        return Table

    async def get_context(self):
        return DBContext(self.pool)

    async def __list_tables(self):
        async with await self.get_context() as (conn, curs):
            await curs.execute('''
                SELECT DISTINCT table_name
                FROM information_schema.columns
                WHERE table_schema='public' ''')
            results = [i[0] for i in curs]
        return results

    async def refresh_tables(self):
        """
        Create all Table instances from all tables found in the database.
        """
        if self.keys():
            # Reset this DictDB because it contains old tables
            super(DictDB, self).__init__()
        table_cls = self.table_factory()
        for table_name in await self.__list_tables():
            self[table_name] = await table_cls(table_name, self).init()


_json_column_types = ('json', 'jsonb')


class Table(dictorm.Table):

    def __init__(self, table_name, db: DictDB):
        self.name = table_name
        self.db = db
        self.pks = []
        self.refs = {}
        self.order_by = None
        self.fks = {}
        self.cached_columns_info = None
        self.cached_column_names = None

    async def init(self):
        await self._refresh_pks()
        # Detect json column types for this table's columns
        type_column_name = 'type' if self.db.kind == 'sqlite3' else 'data_type'
        data_types = [i[type_column_name].lower() for i in await self.columns_info]
        self.has_json = True if \
            [i for i in _json_column_types if i in data_types] \
            else False
        return self

    async def _refresh_pks(self):
        """
        Get a list of Primary Keys set for this table in the DB.
        """
        async with await self.db.get_context() as (conn, curs):
            await curs.execute(self.__SELECT_PG_PKEYS % self.name)
            self.pks = [i[0] for i in curs]

    @property
    async def columns_info(self):
        """
        Get a dictionary that contains information about all columns of this
        table.
        """
        if self.cached_columns_info:
            return self.cached_columns_info

        async with await self.db.get_context() as (conn, curs):
            sql = f'''
            SELECT
                a.attname as "Column",
                pg_catalog.format_type(a.atttypid, a.atttypmod) as "Datatype"
            FROM
                pg_catalog.pg_attribute a
            WHERE
                a.attnum > 0
                AND NOT a.attisdropped
                AND a.attrelid = (
                    SELECT c.oid
                    FROM pg_catalog.pg_class c
                        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname ~ '^({self.name})$'
                        AND pg_catalog.pg_table_is_visible(c.oid)
                )'''
            await curs.execute(sql)
            self.cached_columns_info = {k: v for k, v in curs}
        return self.cached_columns_info

    @classmethod
    def _dict_factory(cls):
        return Dict


class Dict(dictorm.Dict):

    async def flush(self):
        items = self._get_db_items()
        items = {k: (await v if asyncio.iscoroutine(v) else v)
                 for k, v in items.items() if k in self._table.column_names}
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
        sql, values = query.build()
        self._curs.execute(sql, values)
        if query._returning:
            return self._curs.fetchone()
