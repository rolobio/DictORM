from psycopg2.extras import DictCursor

__all__ = ['PgPyTable', 'PgPyDict']

def key_value_pairs(d, join_str=', '):
    return join_str.join([str(k)+'=%('+k+')s' for k in d.keys()])


class PgPyTable(object):

    def __init__(self, table, psycopg2_cursor, primary_keys):
        self.table = table
        self.curs = psycopg2_cursor
        self.pks = primary_keys

    def _execute(self, *a, **kw):
        return self.curs.execute(*a, **kw)


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
        return PgPyDict(self.curs.fetchone(), self.table, self.curs, self.pks)


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
        self._execute(
            'SELECT * FROM {} WHERE {}'.format(
                self.table,
                key_value_pairs(primary, join_str=' AND ')),
            primary
            )
        return PgPyDict(self.curs.fetchone(), self.table, self.curs, self.pks)



class PgPyDict(dict):
    """
    A Python dictionary wrapped so that it's contents are flushed to the
    database when a change is made.
    """

    def __init__(self, d, table, curs, primary_keys):
        self._table = table
        self._curs = curs
        self._pks = primary_keys
        i = super().__init__(d)


    def flush(self):
        kvp = key_value_pairs(self)
        self._curs.execute('UPDATE {} SET {} WHERE id=%(id)s'.format(
                self._table, kvp), self)


    def __setitem__(self, key, val):
        """
        Will not modify Primary Key values.
        """
        if key in self._pks:
            return
        ret = super().__setitem__(key, val)
        self.flush()
        return ret


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



