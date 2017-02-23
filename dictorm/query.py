from psycopg2.extensions import cursor

mogrify = cursor.mogrify

class Select(object):

    query = 'SELECT * FROM {table}'

    def __init__(self, table, logicals_or_exp=[], returning=None):
        self.table = table
        self.logicals_or_exp = logicals_or_exp
        self.returning = returning
        self._order_by = None
        self._limit = None
        self._offset= None


    def __str__(self):
        sql = self.query
        formats = {'table':self.table,}
        if self.logicals_or_exp:
            sql += ' WHERE {exp}'
            formats['exp'] = str(self.logicals_or_exp)
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
        return list(self.logicals_or_exp or [])


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
        try:
            self.logicals_or_exp.append(item)
        except AttributeError:
            self.logicals_or_exp = And(self.logicals_or_exp, item)



class Insert(object):

    query = 'INSERT INTO {table} {cvp}'
    cvp = '({}) VALUES ({})'

    def __init__(self, table, **values):
        self.table = table
        self._values = values
        self._returning = None


    def _build_cvp(self):
        return (', '.join([k for k,v in self.sorted_items()]),
            ', '.join(['%s',]*len(self._values)))


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
        return (str(self), self.values())


    def returning(self, returning):
        self._returning = returning
        return self



class Update(Insert):

    query = 'UPDATE {table} SET {cvp}'

    def __init__(self, table, **values):
        self.logicals_or_exp = None
        super().__init__(table, **values)

    def _build_cvp(self):
        return ', '.join(['{}=%s'.format(k) for k,v in self.sorted_items()])

    def __str__(self):
        sql = self.query
        formats = {'table':self.table, 'cvp':self._build_cvp()}
        if self.logicals_or_exp:
            sql += ' WHERE {exps}'
            formats['exps'] = str(self.logicals_or_exp)
        if self._returning:
            sql += ' RETURNING '+str(self._returning)
        return sql.format(**formats)


    def values(self):
        values = super().values()
        if self.logicals_or_exp:
            values.extend(list(self.logicals_or_exp))
        return values


    def where(self, logicals_or_exp):
        self.logicals_or_exp = logicals_or_exp
        return self


class Delete(Update):

    query = 'DELETE FROM {table}'



class Column(object):

    def __init__(self, table, column):
        self.table = table
        self.column = column

    def __repr__(self):
        return 'Column({}.{})'.format(self.table, self.column)

    def __eq__(self, column): return Expression(self, column, '=')
    def __gt__(self, column): return Expression(self, column, '>')
    def __ge__(self, column): return Expression(self, column, '>=')
    def __lt__(self, column): return Expression(self, column, '<')
    def __le__(self, column): return Expression(self, column, '<=')
    def __ne__(self, column): return Expression(self, column, '!=')

    def In(self, tup):
        return Expression(self, tup, ' IN ')



class Expression(object):

    def __init__(self, column1, column2, kind):
        self.column1 = column1
        self.column2 = column2
        self.kind = kind
        self._substratum = None

    def __repr__(self):
        return 'Expression({}{}{})'.format(self.column1,
                self.kind, self.column2)

    def __str__(self):
        c1 = self.column1.column
        return '{}{}%s'.format(c1, self.kind)


    def value(self):
        return self.column2


    def __iter__(self):
        return iter([self.column2,])


    def foreign_key(self):
        return [self.column1.column,
                self.column2.table,
                self.column2.column,
                False if self.kind == '=' else True,
                self._substratum]


    def substratum(self, column):
        self._substratum = column
        return self


    def Or(self, exp2): return Or(self, exp2)
    def Xor(self, exp2): return Xor(self, exp2)
    def And(self, exp2): return And(self, exp2)



class Logical(object):

    def __init__(self, kind, *logicals_or_exp):
        self.kind = kind
        self.logicals_or_exp = list(logicals_or_exp)
        self.extend = self.logicals_or_exp.extend
        self.append = self.logicals_or_exp.append

    def __repr__(self):
        return '{}({})'.format(self.kind, repr(self.logicals_or_exp))

    def __str__(self):
        kind = ' '+self.kind+' '
        s = []
        for exp in self.logicals_or_exp:
            if isinstance(exp, Logical):
                s.append('('+str(exp)+')')
            else:
                s.append(str(exp))
        return kind.join(s)


    def __iter__(self):
        i = []
        for exp in self.logicals_or_exp:
            if isinstance(exp, Logical):
                i.extend(list(exp))
            elif isinstance(exp, Expression):
                i.append(exp.value())
            else:
                i.append(exp)
        return iter(i)



class Or(Logical):
    def __init__(self, *logicals_or_exp):
        super().__init__('OR', *logicals_or_exp)

class Xor(Logical):
    def __init__(self, *logicals_or_exp):
        super().__init__('XOR', *logicals_or_exp)

class And(Logical):
    def __init__(self, *logicals_or_exp):
        super().__init__('AND', *logicals_or_exp)



