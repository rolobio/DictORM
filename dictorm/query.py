
class Select(object):

    query = 'SELECT * FROM {table} WHERE {exp}'

    def __init__(self, table, exp):
        self.table = table
        self.exp = exp

    def __str__(self):
        sql = self.query.format(table=self.table, exp=str(self.exp))
        return sql


class Insert(object):

    query = 'INSERT INTO {table} {cvp}'

    def __init__(self, table, **values):
        self.table = table
        self.values = values



class Column(object):

    def __init__(self, table, column):
        self.table = table
        self.column = column

    def __eq__(self, column): return Expression(self, column, '=')
    def __gt__(self, column): return Expression(self, column, '>')
    def __ge__(self, column): return Expression(self, column, '>=')
    def __lt__(self, column): return Expression(self, column, '<')
    def __le__(self, column): return Expression(self, column, '<=')
    def __ne__(self, column): return Expression(self, column, '!=')



class Expression(object):

    def __init__(self, column1, column2, kind):
        self.column1 = column1
        self.column2 = column2
        self.kind = kind

    def __str__(self):
        c1 = self.column1.column
        if isinstance(self.column2, str):
            c2 = "'{}'".format(self.column2)
        elif isinstance(self.column2, (int, float)):
            c2 = str(self.column2)
        return '{}{}{}'.format(c1, self.kind, c2)

    def Or(self, exp2): return Or(self, exp2)
    def Xor(self, exp2): return Xor(self, exp2)
    def And(self, exp2): return And(self, exp2)



class Logical(object):

    def __init__(self, kind, *exps_or_logicals):
        self.kind = kind
        self.exps_or_logicals = exps_or_logicals

    def __str__(self):
        kind = ' '+self.kind+' '
        s = []
        for exp in self.exps_or_logicals:
            if isinstance(exp, Logical):
                s.append('('+str(exp)+')')
            else:
                s.append(str(exp))
        return kind.join(s)



class Or(Logical):
    def __init__(self, *exps_or_logicals):
        super().__init__('OR', *exps_or_logicals)

class Xor(Logical):
    def __init__(self, *exps_or_logicals):
        super().__init__('XOR', *exps_or_logicals)

class And(Logical):
    def __init__(self, *exps_or_logicals):
        super().__init__('AND', *exps_or_logicals)



