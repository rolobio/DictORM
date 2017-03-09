"""
This module is used to build queries for Postgresql.  You shouldn't really need
to import anything from this file because they can all be built using
dictorm.Table.

Sqlite queries are slightly different, but use these methods as their base.
"""
from psycopg2.extensions import cursor

mogrify = cursor.mogrify

class Select(object):

    query = 'SELECT * FROM {table}'

    def __init__(self, table, operators_or_comp=None, returning=None):
        self.table = table
        self.operators_or_comp = operators_or_comp or []
        self.returning = returning
        self._order_by = None
        self._limit = None
        self._offset= None


    def __repr__(self): # pragma: no cover
        return 'Select({}, {}, ret:{}, order:{}, limit:{}, offset:{}'.format(
                self.table,
                repr(self.operators_or_comp),
                self.returning,
                self._order_by,
                self._limit,
                self._offset)


    def __str__(self):
        sql = self.query
        formats = {'table':self.table,}
        oc = self.operators_or_comp
        if (isinstance(oc, Operator) and oc.operators_or_comp) or (
                isinstance(oc, Comparison)
                ):
            sql += ' WHERE {comp}'
            formats['comp'] = str(oc)
        if self._order_by:
            sql += ' ORDER BY '+str(self._order_by)
        if self.returning:
            sql += ' RETURNING '+str(self.returning)
        if self._limit:
            sql += ' LIMIT '+str(self._limit)
        if self._offset:
            sql += ' OFFSET '+str(self._offset)
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


    def append(self, item):
        self.operators_or_comp.append(item)



class Insert(object):

    query = 'INSERT INTO {table} {cvp}'
    cvp = '({}) VALUES ({})'
    interpolation_str = '%s'
    append_returning = None
    last_row = 'SELECT {} FROM {} WHERE rowid = last_insert_rowid()'

    def __init__(self, table, **values):
        self.table = table
        self._values = values
        self._returning = None


    def _build_cvp(self):
        return (', '.join([k for k,v in self.sorted_items()]),
            ', '.join([self.interpolation_str,]*len(self._values)))


    def __str__(self):
        sql = self.query
        if self._returning:
            sql += ' RETURNING '+str(self._returning)
        if not self._values:
            return sql.format(table=self.table, cvp='DEFAULT VALUES')
        return sql.format(table=self.table,
                cvp=self.cvp.format(*self._build_cvp()))


    def sorted_items(self):
        return sorted(self._values.items())


    def values(self):
        return [self._values[k] for k in sorted(self._values)]


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

    query = 'UPDATE {table} SET {cvp}'
    interpolation_str = '%s'

    def __init__(self, table, **values):
        self.operators_or_comp = None
        super(Update, self).__init__(table, **values)


    def _build_cvp(self):
        return ', '.join(['{}={}'.format(k, self.interpolation_str) \
                for k,v in self.sorted_items()])

    def __str__(self):
        sql = self.query
        formats = {'table':self.table, 'cvp':self._build_cvp()}
        if self.operators_or_comp:
            sql += ' WHERE {comps}'
            formats['comps'] = str(self.operators_or_comp)
        if self._returning:
            sql += ' RETURNING '+str(self._returning)
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

    query = 'DELETE FROM {table}'


class Comparison(object):

    interpolation_str = '%s'
    many = False

    def __init__(self, column1, column2, kind):
        self.column1 = column1
        self.column2 = column2
        self.kind = kind
        self._substratum = None

    def __repr__(self): # pragma: no cover
        if isinstance(self.column2, Null):
            ret = 'Comparison({}{})'.format(self.column1, self.kind)
        ret = 'Comparison({}{}{})'.format(self.column1,
                str(self.kind), self.column2)
        if self._substratum:
            ret += '.substratum({})'.format(self._substratum)
        return ret


    def __str__(self):
        c1 = self.column1.column
        if self._null_kind():
            return '{}{}'.format(c1, self.kind)
        return '{}{}{}'.format(c1, self.kind, self.interpolation_str)


    def value(self):
        return self.column2


    def __iter__(self):
        if self._null_kind():
            return iter([])
        return iter([self.column2,])


    def substratum(self, column):
        self._substratum = column
        return self


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
        return '{}.{}'.format(self.table.name, self.column)

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



class Operator(object):

    def __init__(self, kind, *operators_or_comp):
        self.kind = kind
        self.operators_or_comp = list(operators_or_comp)

    def __repr__(self): # pragma: no cover
        return '{}({})'.format(self.kind, repr(self.operators_or_comp))

    def __str__(self):
        kind = ' '+self.kind+' '
        s = []
        for comp in self.operators_or_comp:
            if isinstance(comp, Operator):
                s.append('('+str(comp)+')')
            else:
                s.append(str(comp))
        return kind.join(s)


    def __iter__(self):
        i = []
        for comp in self.operators_or_comp:
            if isinstance(comp, Operator):
                i.extend(list(comp))
            elif isinstance(comp, Comparison) and not comp._null_kind():
                i.append(comp.value())
        return iter(i)


    def extend(self, l): return self.operators_or_comp.extend(l)
    def append(self, i): return self.operators_or_comp.append(i)



class Or(Operator):
    def __init__(self, *operators_or_comp):
        super(Or, self).__init__('OR', *operators_or_comp)

class Xor(Operator):
    def __init__(self, *operators_or_comp):
        super(Xor, self).__init__('XOR', *operators_or_comp)

class And(Operator):
    def __init__(self, *operators_or_comp):
        super(And, self).__init__('AND', *operators_or_comp)



