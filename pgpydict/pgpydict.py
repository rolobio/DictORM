from psycopg2.extras import DictCursor

__all__ = ['PgPyTable', 'PgPyDict']

def column_value_pairs(d, join_str=', '):
    """
    Create a string of SQL that will instruct a Psycopg2 DictCursor to
    interpolate the dictionary's keys into a SELECT or UPDATE SQL query.

    Example:
        d = {'id':10, 'person':'Dave'}

        becomes

        id=%(id)s, person=%(person)s
    """
    return join_str.join([str(k)+'=%('+k+')s' for k in d.keys()])


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


class PgPyTable(object):
    """
    I am used to specify how a Psycopg2 table is constructed.  My init'd object
    can later be called to insert objects into that table.

    Example 1:
        Postgresql Table:
            >>> CREATE TABLE my_table (id SERIAL PRIMARY KEY, foo TEXT)

        Python:
            >>> conn = psycopg2.connect(**db_config)
            >>> curs = conn.cursor(cursor_factory=DictCursor)
            >>> MyTable = PgPyTable('my_table', curs, ('id',))
            >>> MyTable
            PgPyTable(my_table, ('id',))

            >>> row1 = MyTable({'foo':'bar',})
            {'id':1, 'foo':'bar'}

            >>> row2 = MyTable({'foo':'baz',})
            {'id':2, 'foo':'baz'}

            >>> row2_copy = MyTable.getByPrimary(2)
            >>> row2 == row2__copy
            True
    """

    def __init__(self, table, psycopg2_cursor, primary_keys):
        self.table = table
        self.curs = psycopg2_cursor
        self.pks = primary_keys
        self.ref_info = {}


    def __repr__(self):
        return 'PgPyTable({}, {})'.format(self.table, self.pks)


    def _execute(self, *a, **kw):
        return self.curs.execute(*a, **kw)


    def _initPgPyDict(self, d):
        # Create the PgPyDict before appending references because dictcursor
        # will not allow columns to be added.
        d = PgPyDict(d, self.table, self.curs, self.pks,
                self.ref_info)
        d = self._appendReferences(d)
        return d


    def __call__(self, *a, **kw):
        """
        Insert a new row using the provided dictionary.
        """
        d = dict(*a, **kw)
        keys = d.keys()
        if keys:
            self._execute('INSERT INTO {} {} RETURNING *'.format(
                        self.table, insert_column_value_pairs(keys)),
                    d)
        else:
            # Handle an empty call
            self._execute('INSERT INTO {} DEFAULT VALUES RETURNING *'.format(
                self.table))
        return self._initPgPyDict(self.curs.fetchone())


    def getByPrimary(self, primary):
        """
        Get a row from the table that matches the primary keys specified
        during this instance's creation.

        Single Primary Key:
            t = PgPyTable('some_table', curs, ('id',))
            row = t.getByPrimary(42)

        Multiple Primary Keys:
            t = PgPyTable('some_table', curs, ('id', 'group_id'))
            row = t.getByPrimary({'id':8, 'group_id':12})
        """
        try:
            if type(primary) != dict:
                primary = {self.pks[0]:primary,}
            return self._initPgPyDict(self.getWhere(primary))
        except IndexError:
            raise ValueError('No primary keys specified, use getWhere')


    def getWhere(self, where):
        self._execute('SELECT * FROM {} WHERE {}'.format(
                self.table,
                column_value_pairs(where, join_str=' AND ')
                ),
            where)
        return self.curs.fetchone()


    def _appendReferences(self, d):
        for ref_column in self.ref_info:
            table, pk, key_name = self.ref_info[ref_column]
            d[key_name] = table.getWhere({pk:d[ref_column],})
        return d


    def addReference(self, pgpytable, primary_key, ref_column, key_name):
        """
        When getting this table's row, get another table's row which is
        referenced by the provided parameters.
        """
        self.ref_info[ref_column] = (pgpytable, primary_key, key_name)



class PgPyDict(dict):
    """
    A Python dictionary wrapped so that it's contents are flushed to the
    database when a change is made.
    """

    def __init__(self, d, table, curs, primary_keys, references):
        self._table = table
        self._curs = curs
        self._pks = primary_keys
        self._ref_info = references
        self._primary_to_ref = {}
        for ref_column in self._ref_info:
            i,j,k = self._ref_info[ref_column]
            self._primary_to_ref[k] = ref_column
        super().__init__(d)


    def flush(self):
        if self._ref_info:
            # Do not send custom columns to the database
            kvp = column_value_pairs(dict([(k,v) for k,v in self.items() if k not in self._primary_to_ref]))
        else:
            kvp = column_value_pairs(self)
        self._curs.execute('UPDATE {} SET {} WHERE id=%(id)s'.format(
                self._table, kvp), self)


    def __setitem__(self, key, val):
        """
        Will not modify Primary Key values.
        """
        if key in self._pks:
            return
        elif key in self._primary_to_ref:
            if type(val) == PgPyDict:
                # Use the Dict object to set it's matching primary key
                pgpytable, primary_key, key_name = self._ref_info[self._primary_to_ref[key]]
                super().__setitem__(self._primary_to_ref[key], val[primary_key])
            else:
                super().__setitem__(self._primary_to_ref[key], val)
        elif key in self._ref_info:
            # Set the matching dict to None, it will be looked-up if it is ever
            # requested using __getitem__
            pgpytable, primary_key, key_name = self._ref_info[key]
            super().__setitem__(key_name, None)
        super().__setitem__(key, val)
        self.flush()


    def __getitem__(self, key):
        """
        Will retrieve a sub-PgPyDict if it has not yet been retrieved.
        """
        if key in self._primary_to_ref and not super().__getitem__(key):
            # The primary key has not yet been set, get it from the referenced
            # object.
            ref = self._primary_to_ref[key]
            pgpytable, primary_key, key_name = self._ref_info[ref]
            if super().__getitem__(ref):
                super().__setitem__(
                        key,
                        pgpytable.getByPrimary(super().__getitem__(ref)))
        elif key in self._ref_info and not super().__getitem__(key):
            pgpytable, primary_key, key_name = self._ref_info[key]
            if super().get(key_name):
                # Object has already been set, use its primary key
                super().__setitem__(
                        key,
                        super().__getitem__(key_name)[primary_key])
        return super().__getitem__(key)


    def __delitem__(self, key):
        """
        Instead of deleting an item, I will set it to None so that it's new
        value will be null in the database.
        """
        self[key] = None
        self.flush()


    def update(self, d):
        """
        Will not modify Primary Key values.
        """
        ret = super().update([(k,v) for k,v in d.items() if k not in self._pks])
        self.flush()
        return ret


    __delitem__.__doc__ += dict.__delitem__.__doc__
    __setitem__.__doc__ += dict.__setitem__.__doc__
    __getitem__.__doc__ += dict.__getitem__.__doc__
    update.__doc__ += dict.update.__doc__



