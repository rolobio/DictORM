from dictorm import pg

Select = pg.Select
Delete = pg.Delete


class Comparison(pg.Comparison):
    interpolation_str = '$'


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
