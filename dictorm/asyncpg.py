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

    async def values(self):
        return [self._values[k] for k in self._ordered_keys]

    async def build(self):
        try:
            sql, values = str(self), await self.values()
        except TypeError:
            sql, values = await self.str(), await self.values()
        if self.append_returning:
            ret = [(sql, values), ]
            ret.append((self.last_row.format(
                self.append_returning, self.table),
                        []))
            return ret
        return (sql, values)


class Update(pg.Update):
    interpolation_str = '$'

    values = Insert.values
    build = Insert.build

    async def str(self):
        parts = []
        formats = {'table': self.table, 'cvp': self._build_cvp()}
        if self.operators_or_comp:
            parts.append(' WHERE {comps}')
            formats['comps'] = self.operators_or_comp.str(var_offset=len(await self.values()))
        if self._returning == '*':
            parts.append(' RETURNING *')
        elif self._returning:
            parts.append(' RETURNING "{0}"'.format(self._returning))
        sql = self.query + ''.join(parts)
        return sql.format(**formats)

    def _build_cvp(self):
        return ', '.join(('"{}"={}{}'.format(k, self.interpolation_str, i+1)
                          for i, k in enumerate(self._ordered_keys)))
