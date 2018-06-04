#
# downcast - tools for unpacking patient data from DWC
#
# Copyright (c) 2018 Laboratory for Computational Physiology
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from ..exceptions import (Error, DataError, ProgrammingError)

class BCPCursor:
    def __init__(self, connection):
        self._conn = connection
        self._table_iters = {}
        self._query_fetch = None
        self._query_skip = None
        self._query_cols = None
        self.description = None
        self.rowcount = -1
        self.arraysize = 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row:
            return row
        else:
            raise StopIteration()

    #### DB-API ####

    def close(self):
        try:
            for it in self._table_iters.values():
                it.close()
        finally:
            self._table_iters = {}
            self._conn = None
            self._query_fetch = None
            self._query_skip = None
            self._query_cols = None

    def execute(self, statement, params = ()):
        try:
            q = self._conn.parse(statement, params)
        except Error:
            raise
        except Exception as e:
            raise ProgrammingError(e)

        table = self._conn.get_table(q.table)

        if table not in self._table_iters:
            self._table_iters[table] = table.iterator()
        it = self._table_iters[table]

        if q.order is not None:
            i = table.column_number(q.order)
            if i != table.order_column():
                raise ProgrammingError('cannot sort %s by %s'
                                       % (q.table, q.order))

        cols = []
        for c in q.columns:
            if c == '*':
                cols += range(table.n_columns())
            else:
                cols.append(table.column_number(c))

        seek = None
        skip = []
        for c in q.constraints:
            i = table.column_number(c.column)
            t = table.column_type(i)

            try:
                v = t.from_param(c.value)
            except Exception:
                raise ProgrammingError('in %s, cannot compare %s to %r'
                                       % (table.name, c.column, c.value))

            oc = table.order_column()
            rel = c.relation
            if i == oc and rel == '<':
                skip += [_halt_unless(i, rel, v)]
            elif i == oc and rel == '<=':
                skip += [_halt_unless(i, rel, v)]
            elif i == oc and rel == '=' and seek is None:
                seek = (i, v)
                skip += [_halt_unless(i, rel, v)]
            elif i == oc and rel == '>=' and seek is None:
                seek = (i, v)
            elif i == oc and rel == '>' and seek is None:
                seek = (i, v)
                skip += [_skip_unless(i, '<>', v)]
            elif table.column_indexed(i) and rel == '=' and seek is None:
                seek = (i, v)
                skip += [_halt_unless(i, '=', v)]
            else:
                skip += [_skip_unless(i, rel, v)]

        self.description = []
        for i in cols:
            self.description.append((table.column_name(i),
                                     table.column_type(i),
                                     None, None, None, None, None))
        self.rowcount = 0

        if q.limit is not None:
            skip += [lambda r: self.rowcount >= q.limit and _halt()]

        if seek is None:
            it.seek(None, None)
        else:
            it.seek(*seek)
        self._query_fetch = it.fetch
        self._query_skip = skip
        self._query_cols = cols

    def executemany(self, statement, params):
        for p in params:
            self.execute(statement, p)

    def fetchone(self):
        fetch = self._query_fetch
        skip = self._query_skip
        try:
            r = fetch()
            while r:
                if any(f(r) for f in skip):
                    r = fetch()
                else:
                    self.rowcount += 1
                    return [r[i] for i in self._query_cols]
        except HaltQuery:
            self._query_fetch = lambda: None
            return
        except Error:
            self._query_fetch = lambda: None
            raise
        except Exception as e:
            self._query_fetch = lambda: None
            raise DataError(e)

    def fetchmany(self, size = None):
        if size is None:
            size = self.arraysize
        rows = []
        while size > 0:
            size -= 1
            row = self.fetchone()
            if not row:
                break
            rows.append(row)
        return rows

    def fetchall(self):
        rows = []
        row = self.fetchone()
        while row:
            rows.append(row)
            row = self.fetchone()
        return rows

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column):
        pass

    def nextset(self):
        return None

class HaltQuery(Exception):
    pass

def _halt():
    raise HaltQuery()

def _skip_unless(col, rel, value):
    if rel == '<':
        return lambda row: row[col] >= value
    elif rel == '<=':
        return lambda row: row[col] > value
    elif rel == '>':
        return lambda row: row[col] <= value
    elif rel == '>=':
        return lambda row: row[col] < value
    elif rel == '=':
        return lambda row: row[col] != value
    elif rel == '<>':
        return lambda row: row[col] == value
    else:
        raise ProgrammingError('unknown relation %r' % rel)

def _halt_unless(col, rel, value):
    if rel == '<':
        return lambda row: row[col] >= value and _halt()
    elif rel == '<=':
        return lambda row: row[col] > value and _halt()
    elif rel == '>':
        return lambda row: row[col] <= value and _halt()
    elif rel == '>=':
        return lambda row: row[col] < value and _halt()
    elif rel == '=':
        return lambda row: row[col] != value and _halt()
    elif rel == '<>':
        return lambda row: row[col] == value and _halt()
    else:
        raise ProgrammingError('unknown relation %r' % rel)
