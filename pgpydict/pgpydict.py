from psycopg2.extras import DictCursor

__all__ = ['PgPyTable', 'PgPyDict']

def key_value_pairs(d, join_str=', '):
    return join_str.join([str(k)+'=%('+k+')s' for k in d.keys()])


class PgPyTable(object):

    def __init__(self, table, psycopg2_cursor, primary_keys):
        self.table = table
        self.curs = psycopg2_cursor
        self.pks = primary_keys
        self.references_by_ref_column = {}

    def _execute(self, *a, **kw):
        return self.curs.execute(*a, **kw)


    def _initPgPyDict(self, d):
        # Create the PgPyDict before appending references because dictcursor
        # will not allow columns to be added.
        d = PgPyDict(d, self.table, self.curs, self.pks,
                self.references_by_ref_column)
        d = self._appendReferences(d)
        return d


    def __call__(self, *a, **kw):
        """
        Insert a new row using the provided dictionary.
        """
        d = dict(*a, **kw)
        keys = d.keys()
        if keys:
            self._execute('INSERT INTO {} ({}) VALUES ({}) RETURNING *'.format(
                    self.table,
                    ', '.join(keys),
                    ', '.join(['%('+k+')s' for k in keys]),
                    ),
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
        if type(primary) != dict:
            primary = {self.pks[0]:primary,}
        return self._initPgPyDict(self.getWhere(primary))


    def getWhere(self, where):
        self._execute('SELECT * FROM {} WHERE {}'.format(
                self.table,
                key_value_pairs(where, join_str=' AND ')
                ),
            where)
        return self.curs.fetchone()


    def _appendReferences(self, d):
        for ref_column in self.references_by_ref_column:
            table, pk, key_name = self.references_by_ref_column[ref_column]
            d[key_name] = table.getWhere({pk:d[ref_column],})
        return d


    def addReference(self, pgpytable, primary_key, ref_column, key_name):
        """
        When getting this table's row, get another table's row which is
        referenced by the provided parameters.
        """
        self.references_by_ref_column[ref_column] = (pgpytable, primary_key, key_name)



class PgPyDict(dict):
    """
    A Python dictionary wrapped so that it's contents are flushed to the
    database when a change is made.
    """

    def __init__(self, d, table, curs, primary_keys, references):
        self._table = table
        self._curs = curs
        self._pks = primary_keys
        self._references_by_ref_column = references
        self._references_by_primary = {}
        for ref_column in self._references_by_ref_column:
            i,j,k = self._references_by_ref_column[ref_column]
            self._references_by_primary[k] = ref_column
        super().__init__(d)


    def flush(self):
        if self._references_by_ref_column:
            # Do not send custom columns to the database
            kvp = key_value_pairs(dict([(k,v) for k,v in self.items() if k not in self._references_by_primary]))
        else:
            kvp = key_value_pairs(self)
        self._curs.execute('UPDATE {} SET {} WHERE id=%(id)s'.format(
                self._table, kvp), self)


    def __setitem__(self, key, val):
        """
        Will not modify Primary Key values.
        """
        if key in self._pks:
            return
        elif key in self._references_by_primary:
            if type(val) == PgPyDict:
                # Use the Dict object to set it's matching primary key
                pgpytable, primary_key, key_name = self._references_by_ref_column[self._references_by_primary[key]]
                super().__setitem__(self._references_by_primary[key], val[primary_key])
            else:
                super().__setitem__(self._references_by_primary[key], val)
        elif key in self._references_by_ref_column:
            # Set the matching dict to None, it will be looked-up if it is ever
            # requested using __getitem__
            pgpytable, primary_key, key_name = self._references_by_ref_column[key]
            super().__setitem__(key_name, None)
        super().__setitem__(key, val)
        self.flush()


    def __getitem__(self, key):
        if key in self._references_by_primary and not super().__getitem__(key):
            # The primary key has not yet been set, get it from the referenced
            # object.
            ref = self._references_by_primary[key]
            pgpytable, primary_key, key_name = self._references_by_ref_column[ref]
            if super().__getitem__(ref):
                super().__setitem__(
                        key,
                        pgpytable.getByPrimary(super().__getitem__(ref)))
        elif key in self._references_by_ref_column and not super().__getitem__(key):
            pgpytable, primary_key, key_name = self._references_by_ref_column[key]
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
    update.__doc__ += dict.update.__doc__



