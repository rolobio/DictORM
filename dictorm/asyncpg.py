from dictorm import pg

And = pg.And
Delete = pg.Delete
Null = pg.Null
Operator = pg.Operator
Or = pg.Or
Select = pg.Select


class Comparison(pg.Comparison):
    interpolation_str = '$'

    def str(self, var_offset=''):
        c1 = self.column1.column

        if self._null_kind():
            return '"{}"{}'.format(c1, self.kind)

        # Surround the expression with parentheses
        if self._array_exp:
            return '"{}"{}({}{})'.format(c1, self.kind, self.interpolation_str, var_offset)

        return '"{}"{}{}{}'.format(c1, self.kind, self.interpolation_str, var_offset)


class Column(pg.Column):
    comparison = Comparison


class Insert(pg.Insert):
    interpolation_str = '$'

    def _build_cvp(self):
        columns = ', '.join(['"{}"'.format(i) for i in self._ordered_keys])
        values = ', '.join(['{}{}'.format(self.interpolation_str, i+1) for i in
                            range(len(self._values))])
        return columns, values


class Update(pg.Update):
    interpolation_str = '$'

    def _build_cvp(self):
        return ', '.join(('"{}"={}{}'.format(k, self.interpolation_str, i+1)
                          for i, k in enumerate(self._ordered_keys)))
