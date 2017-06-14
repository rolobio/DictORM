"""
This module is used to build queries for Postgresql.  You shouldn't really need
to import anything from this file because they can all be built using
dictorm.Table.

Sqlite queries are slightly different, but use these methods as their base.
"""
from psycopg2.extensions import cursor

mogrify = cursor.mogrify

global sort_keys
sort_keys = False
def set_sort_keys(val):
    "Used only for testing"
    global sort_keys
    sort_keys = val


class Select(object):

    query = 'SELECT * FROM "{table}"'

    def __init__(self, table, operators_or_comp=None, returning=None):
        self.table = table
        self.operators_or_comp = operators_or_comp or []
        self.returning = returning
        self._order_by = None
        self._limit = None
        self._offset= None


    def __repr__(self): # pragma: no cover
        return 'Select({0}, {1}, ret:{2}, order:{3}, limit:{4}, offset:{5}'.format(
                self.table,
                repr(self.operators_or_comp),
                self.returning,
                self._order_by,
                self._limit,
                self._offset)


    def __str__(self):
        parts = []
        formats = {'table':self.table,}
        ooc = self.operators_or_comp
        if (isinstance(ooc, Operator) and ooc.operators_or_comp) or (
                isinstance(ooc, Comparison)
                ):
            parts.append(' WHERE {comp}')
            formats['comp'] = str(ooc)
        if self._order_by:
            parts.append(' ORDER BY {0}'.format(str(self._order_by)))
        if self.returning == '*':
            parts.append(' RETURNING *')
        elif self.returning:
            parts.append(' RETURNING "{0}"'.format(str(self.returning)))
        if self._limit:
            parts.append(' LIMIT {0}'.format(str(self._limit)))
        if self._offset:
            parts.append(' OFFSET {0}'.format(str(self._offset)))
        sql = self.query + ''.join(parts)
        return sql.format(**formats)


    def values(self):
        return list(self.operators_or_comp or [])


    def build(self):
        return (str(self), self.values())


    def order_by(self, order_by):
        self._order_by = order_by
        return self


    def limit(self, limit):
        self._limit = limit
        return self


    def offset(self, offset):
        self._offset = offset
        return self


    def __add__(self, item):
        self.operators_or_comp += item
        return self



class Insert(object):

    query = 'INSERT INTO "{table}" {cvp}'
    cvp = '({0}) VALUES ({1})'
    interpolation_str = '%s'
    append_returning = None
    last_row = 'SELECT {0} FROM "{1}" WHERE "rowid" = last_insert_rowid()'

    def __init__(self, table, **values):
        self.table = table
        self._values = values
        self._returning = None
        self._ordered_keys = values.keys()
        if sort_keys:
            self._ordered_keys = sorted(self._ordered_keys)


    def _build_cvp(self):
        return (', '.join(['"{}"'.format(i) for i in self._ordered_keys]),
            ', '.join([self.interpolation_str,]*len(self._values)))


    def __str__(self):
        sql = self.query
        if self._returning == '*':
            sql += ' RETURNING *'
        elif self._returning:
            sql += ' RETURNING "{0}"'.format(self._returning)
        if not self._values:
            return sql.format(table=self.table, cvp='DEFAULT VALUES')
        return sql.format(table=self.table,
                cvp=self.cvp.format(*self._build_cvp()))


    def values(self):
        return [self._values[k] for k in self._ordered_keys]


    def build(self):
        sql, values = str(self), self.values()
        if self.append_returning:
            ret = [(sql, values),]
            ret.append((self.last_row.format(
                self.append_returning, self.table),
                []))
            return ret
        return (sql, values)


    def returning(self, returning):
        self._returning = returning
        return self



class Update(Insert):

    query = 'UPDATE "{table}" SET {cvp}'
    interpolation_str = '%s'

    def __init__(self, table, **values):
        self.operators_or_comp = None
        super(Update, self).__init__(table, **values)


    def _build_cvp(self):
        return ', '.join(('"{0}"={1}'.format(k, self.interpolation_str) \
                for k in self._ordered_keys))

    def __str__(self):
        parts = []
        formats = {'table':self.table, 'cvp':self._build_cvp()}
        if self.operators_or_comp:
            parts.append(' WHERE {comps}')
            formats['comps'] = str(self.operators_or_comp)
        if self._returning == '*':
            parts.append(' RETURNING *')
        elif self._returning:
            parts.append(' RETURNING "{0}"'.format(self._returning))
        sql = self.query + ''.join(parts)
        return sql.format(**formats)


    def values(self):
        values = super(Update, self).values()
        if self.operators_or_comp:
            values.extend(list(self.operators_or_comp))
        return values

    def where(self, operators_or_comp):
        self.operators_or_comp = operators_or_comp
        return self



class Delete(Update):

    query = 'DELETE FROM "{table}"'


class Comparison(object):

    interpolation_str = '%s'
    many = False

    def __init__(self, column1, column2, kind):
        self.column1 = column1
        self.column2 = column2
        self.kind = kind
        self._substratum = None
        self._aggregate = False

    def __repr__(self): # pragma: no cover
        if isinstance(self.column2, Null):
            ret = 'Comparison({0}{1})'.format(self.column1, self.kind)
        ret = 'Comparison{0}({1}{2}{3})'.format('Many' if self.many else '',
                self.column1, str(self.kind), self.column2)
        if self._substratum:
            ret += '.substratum({0})'.format(self._substratum)
        return ret


    def __str__(self):
        c1 = self.column1.column
        if self._null_kind():
            return '"{0}"{1}'.format(c1, self.kind)
        return '"{0}"{1}{2}'.format(c1, self.kind, self.interpolation_str)


    def value(self):
        return self.column2


    def __iter__(self):
        if self._null_kind():
            return iter([])
        return iter([self.column2,])


    def substratum(self, column):
        comp = Comparison(self.column1, self.column2, self.kind)
        comp._substratum = column
        comp.many = self.many
        return comp


    def aggregate(self, column):
        comp = self.substratum(column)
        comp._aggregate = True
        return comp


    def _null_kind(self): return isinstance(self.column2, Null)


    def Or(self, comp2): return Or(self, comp2)
    def Xor(self, comp2): return Xor(self, comp2)
    def And(self, comp2): return And(self, comp2)



class Null(): pass



class Column(object):

    comparison = Comparison

    def __init__(self, table, column):
        self.table = table
        self.column = column

    def __repr__(self): # pragma: no cover
        return '{0}.{1}'.format(self.table.name, self.column)

    def many(self, column):
        c = self.comparison(self, column, '=')
        c.many = True
        return c

    def __eq__(self, column): return self.comparison(self, column, '=')
    def __gt__(self, column): return self.comparison(self, column, '>')
    def __ge__(self, column): return self.comparison(self, column, '>=')
    def __lt__(self, column): return self.comparison(self, column, '<')
    def __le__(self, column): return self.comparison(self, column, '<=')
    def __ne__(self, column): return self.comparison(self, column, '!=')
    def Is(self, column):     return self.comparison(self, column, ' IS ')
    def IsNot(self, column):  return self.comparison(self, column, ' IS NOT ')

    def IsDistinct(self, column):
        return self.comparison(self, column, ' IS DISTINCT FROM ')


    def IsNotDistinct(self, column):
        return self.comparison(self, column, ' IS NOT DISTINCT FROM ')


    def IsNull(self):
        return self.comparison(self, Null(), ' IS NULL')


    def IsNotNull(self):
        return self.comparison(self, Null(), ' IS NOT NULL')


    def In(self, tup):
        if isinstance(tup, list):
            tup = tuple(tup)
        return self.comparison(self, tup, ' IN ')


    def Like(self, column):
        return self.comparison(self, column, ' LIKE ')


    def Ilike(self, column):
        return self.comparison(self, column, ' ILIKE ')



def wrap_ooc(ooc):
    if isinstance(ooc, Comparison):
        return '%s' % str(ooc)
    return '(%s)' % str(ooc)


class Operator(object):

    def __init__(self, kind, operators_or_comp):
        self.kind = kind
        self.operators_or_comp = operators_or_comp

    def __repr__(self): # pragma: no cover
        return '{0}{1}'.format(self.kind, repr(self.operators_or_comp))

    def __str__(self):
        kind = ' {0} '.format(self.kind)
        return kind.join(map(wrap_ooc, self.operators_or_comp))


    def __iter__(self):
        i = []
        for comp in self.operators_or_comp:
            if isinstance(comp, Operator):
                i.extend(comp)
            elif isinstance(comp, Comparison) and not comp._null_kind():
                i.append(comp.value())
        return iter(i)


    def __add__(self, i):
        if isinstance(i, tuple):
            self.operators_or_comp += i
        else:
            self.operators_or_comp += (i,)
        return self



class Or(Operator):
    def __init__(self, *operators_or_comp):
        super(Or, self).__init__('OR', operators_or_comp)

class Xor(Operator):
    def __init__(self, *operators_or_comp):
        super(Xor, self).__init__('XOR', operators_or_comp)

class And(Operator):
    def __init__(self, *operators_or_comp):
        super(And, self).__init__('AND', operators_or_comp)



