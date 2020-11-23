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

import os
import re
import bisect
import struct

from ..query import SimpleQueryParser
from ..exceptions import (Error, OperationalError,
                          DataSyntaxError, ProgrammingError)
from .cursor import BCPCursor

class BCPConnection:
    """
    Connection to a read-only database of BCP-format files.

    This object provides a standard DB-API interface, for a database
    consisting of plain text/binary files (such as those created by
    'freebcp'.)

    This interface supports only a very limited subset of SQL: it only
    permits simple queries from a single table at a time, with rows
    retrieved in the order they are stored in the underlying file.
    """

    def __init__(self):
        self._tables = {}
        self._parser = SimpleQueryParser()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def add_table(self, name):
        """Add a table to the database."""
        if name not in self._tables:
            self._tables[name] = BCPTable(name)
        return self._tables[name]

    def get_table(self, name):
        """Retrieve a table by name."""
        if name not in self._tables:
            raise OperationalError('undefined table %s' % name)
        return self._tables[name]

    def parse(self, statement, params):
        """Parse an SQL statement."""
        return self._parser.parse(statement, params)

    #### DB-API ####

    def cursor(self):
        """Create a cursor for reading the database."""
        return BCPCursor(self)

    def close(self):
        """Close the database and free internal data."""
        for t in self._tables.values():
            t.clear()
        self._tables = {}

    def commit(self):
        """Commit all changes to the database."""
        pass

class BCPTable:
    """
    A single table in a BCP-format database.

    After creating a table, call add_column() for each column, as well
    as set_order(), set_sync_pattern(), and/or add_index() as
    necessary.

    Then call add_data_file() for each data file.
    """

    def __init__(self, name):
        self.name = name

        self._cname_name = {}
        self._cname_type = {}
        self._cname_num = {}
        self._cname_needindex = set()
        self._order_cname = None

        self._col_name = []
        self._col_format = []
        self._col_type = []
        self._order_column = None
        self._index_columns = set()

        self._sync_pattern = re.compile(b'\n().')
        self._sync_pattern_group = 1

        self._files = []

    def add_column(self, name, data_type):
        """
        Define a column in the table.

        name is the name of the column; data_type is the corresponding
        BCPType.

        This function must be called before importing any data files.
        """
        cname = name.lower()
        self._cname_name[cname] = name
        self._cname_type[cname] = data_type

    def set_order(self, key):
        """
        Define the order of rows in the table.

        This permits efficient queries that are restricted to a range
        of values.

        The order must be a column name.  That column must be non-null
        for every row in the table; every data file must be sorted
        according to that column; data files must be imported in the
        correct order; and consecutive data files may not overlap with
        respect to that column.

        This function must be called before importing any data files.
        """
        if key is None:
            self._order_cname = None
        else:
            self._order_cname = key.lower()

    def add_unique_id(self, key):
        """
        Define an index key for the table.

        This permits efficient queries based on a unique identifier,
        but requires reading the entire data file once in order to
        build an index in memory.

        The key must be a column name.  That column must have a
        distinct value for every row in the table.

        This function must be called before importing any data files.
        """
        self._cname_needindex.add(key.lower())

    def set_sync_pattern(self, pattern, group = 1):
        """
        Set the regular expression used to identify the start of a row.

        pattern is a byte-wise regular expression that should match
        each row boundary.  This pattern is inherently dependent on
        the data file format.

        The location of the row boundary is indicated by the start of
        the first capturing group in the pattern (or the group'th, if
        specified.)

        For example, the default sync pattern, suitable for typical
        plain text files, is b'\n().' (meaning that every row ends
        with a newline and begins with a non-newline character, and
        newlines can never appear within a field.)

        (Note that '^' would not be suitable here, since we are
        matching against arbitrary substrings of the input file.  Note
        also that it doesn't matter whether the pattern matches the
        beginning or end of the file, because the beginning and end
        are always assumed to be row boundaries.)

        This function must be called before importing any data files.
        """
        self._sync_pattern = re.compile(pattern)
        self._sync_pattern_group = group

    def add_data_file(self, data_file, format_file):
        """
        Import a file into the table.

        data_file is the name of the raw data file; format_file is the
        name of the corresponding freebcp format file.  (Note that
        only a very small subset of the possible freebcp formats are
        supported.)

        If multiple data files are supplied, their contents are
        concatenated; the files must have the same format.
        """

        try:
            # Parse the format file
            colname = []
            colfmt = []
            coltype = []
            with open(format_file, 'rt') as fp:
                _ = fp.readline()
                ncols = int(fp.readline())
                for i in range(ncols):
                    info = fp.readline().split()

                    # Each line in the format file describes how a
                    # particular column is stored in the data file.

                    # info[0] = column number
                    # info[1] = data type as stored in the file
                    # info[2] = length of the data length prefix
                    # info[3] = length of the column data
                    # info[4] = string marking the end of the column
                    # info[5] = column number again
                    # info[6] = column name
                    # info[7] = some nonsense

                    cname = info[6].lower()
                    colname.append(self._cname_name[cname])
                    colfmt.append(tuple(info[1:5]))
                    coltype.append(self._cname_type[cname])
        except Exception as e:
            raise OperationalError('error parsing %s: %s' % (format_file, e))

        if self._col_name:
            if colname != self._col_name or colfmt != self._col_format:
                raise OperationalError('format mismatch in %s'
                                       % format_file)

        for (i, name) in enumerate(colname):
            cname = name.lower()
            self._cname_num[cname] = i
            if cname == self._order_cname:
                self._order_column = i
            if cname in self._cname_needindex:
                self._index_columns.add(i)

        self._col_name = colname
        self._col_format = colfmt
        self._col_type = coltype

        # Open the data file and read the first row
        with BCPTableIterator(self, filename = data_file) as it:
            # If the file is empty, ignore it
            row = it._next_row
            if not row:
                return

            # Get the location of the first row, to enable searching
            if self._order_column is None:
                location = None
            else:
                location = row[self._order_column]
                if self._files:
                    (oldfile, oldloc, _, _) = self._files[-1]
                    if location <= oldloc:
                        raise OperationalError(
                            'files out of order (%s, %s)'
                            % (oldfile, data_file))

            row1_offset = it._input_offset()

            # Get the total file size
            f = it._infile
            oldpos = f.tell()
            fsize = f.seek(0, os.SEEK_END)

            # Check that the sync_pattern matches the end of the first row
            # (unless the file contains only one row)
            if row1_offset != fsize:
                f.seek(0)
                b = f.read(row1_offset + 4096)
                m = self._sync_pattern.search(b)
                if not m or m.start(self._sync_pattern_group) != row1_offset:
                    raise DataSyntaxError(
                        'sync pattern not found in first row of %s'
                        % data_file)

            f.seek(oldpos)

            # If any indices are required, read the entire data file
            indices = {}
            if self._index_columns:
                icols = list(self._index_columns)
                for i in icols:
                    indices[i] = {}
                offs = 0
                while row:
                    for i in icols:
                        v = row[i]
                        k = indices[i].setdefault(v, offs)
                        if k != offs:
                            raise OperationalError(
                                'duplicate %s in %s at byte %s and %s'
                                % (self._col_name[i], data_file, k, offs))
                        for (oldfile, _, _, oldind) in self._files:
                            if v in oldind[i]:
                                raise OperationalError(
                                    ('duplicate %s in %s (byte %s)'
                                     + ' and %s (byte %s)')
                                    % (self._col_name[i],
                                       oldfile, oldind[i][v],
                                       data_file, offs))
                    offs = it._input_offset()
                    row = it._fetch_next()

            self._files.append((data_file, location, fsize, indices))

    def n_columns(self):
        """Get the number of columns in the table."""
        return len(self._col_name)

    def column_number(self, name):
        """Get the internal column number for a given column."""
        cname = name.lower()
        if cname not in self._cname_num:
            raise ProgrammingError('no column %s in %s' % (name, self.name))
        return self._cname_num[cname]

    def column_name(self, n):
        """Get the canonical name of the nth column."""
        return self._col_name[n]

    def column_type(self, n):
        """Get the type of the nth column."""
        return self._col_type[n]

    def order_column(self):
        """Get the internal column number of the order column."""
        return self._order_column

    def column_indexed(self, n):
        """Check whether the nth column is indexed."""
        return (n in self._index_columns)

    def clear(self):
        """Remove all imported data."""
        self._files = []

    def iterator(self):
        """Create an iterator for reading the table."""
        return BCPTableIterator(self)

class BCPTableIterator:
    def __init__(self, table, filename = None):
        self._table = table

        # Determine which column is used for ordering; if table is not
        # sorted, it doesn't matter
        if self._table._order_column is None:
            self._loc_column = 0
        else:
            self._loc_column = self._table._order_column

        # Determine the functions used to read data from the input file
        self._readfuncs = []
        readfunc = {
            ('SYBCHAR', '0', '-1', '"\\t"'): self._read_to_tab,
            ('SYBCHAR', '0', '-1', '"\\n"'): self._read_to_lf,
            ('SYBBINARY', '4', '-1', '""'): self._read_blob32,
        }
        for (col, fmt, ty) in zip(table._col_name,
                                  table._col_format,
                                  table._col_type):
            try:
                self._readfuncs.append((readfunc[fmt], ty.from_bytes))
            except KeyError:
                raise OperationalError(
                    'unsupported format for %s in %s'
                    % (col, table.name))

        # Open the given file (if specified), or else open all data
        # files for this table
        if filename is None:
            fnl = [f[0] for f in table._files]
        else:
            fnl = [filename]
        self._infiles = []
        for fn in fnl:
            try:
                f = open(fn, 'rb')
                self._infiles.append(f)
            except Exception as e:
                self.close()
                raise OperationalError('cannot open %s: %s' % (fn, e))
        self._seek_start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        try:
            for f in self._infiles:
                f.close()
        finally:
            self._infiles = []
            self._seek_end()

    def fetch(self):
        """Fetch and return the next row of input data."""
        row = self._next_row
        self._next_row = self._fetch_next()
        return row

    def _fetch_next(self):
        row = []
        for (readf, parsef) in self._readfuncs:
            try:
                bstr = readf()
            except EOFError:
                if len(row) > 0:
                    raise DataSyntaxError(
                        'unexpected EOF while parsing %s in %s'
                        % (self._table._col_name[len(row)],
                           self._input_filename()))
                return None
            except Exception as e:
                raise OperationalError(
                    'error reading %s in %s at byte %s: %s'
                    % (self._table._col_name[len(row)],
                       self._input_filename(),
                       self._input_offset(), e))

            if bstr:
                try:
                    val = parsef(bstr)
                except Exception as e:
                    raise DataSyntaxError(
                        'error parsing %s in %s at byte %s: %s'
                        % (self._table._col_name[len(row)],
                           self._input_filename(),
                           self._input_offset(), e))
                row.append(val)
            else:
                row.append(None)
        return row

    def seek(self, column_number, target):
        """
        Jump to a given position in the input data.

        If target is None, jump to the beginning of the table.
        Otherwise, jump to the first row where the given column
        matches the target value.
        """

        if target is None:
            self._seek_start()
        elif column_number == self._table._order_column:
            self._seek_location(target)
        elif column_number in self._table._index_columns:
            self._seek_indexed(column_number, target)
        else:
            raise ProgrammingError('cannot seek by column %s' % column)

    def _seek_location(self, target):
        # Find the file that contains the given location
        tbl = self._table
        fstart = [f[1] for f in tbl._files]
        filenum = bisect.bisect_right(fstart, target) - 1
        if filenum < 0:
            self._seek_start()
            return

        # Search within this file for the first row >= target
        try:
            start = 0
            end = tbl._files[filenum][2]
            # loop invariants:
            #  - start and end are row boundaries
            #  - every row < start has location < target
            #  - every row >= end has location >= target
            while start < end:
                # search for a row boundary between (start+end)/2 and end
                offs = (start + end) // 2
                roffs = self._sync_input(filenum, offs)
                if roffs is None or roffs >= end:
                    # if we don't find any row boundaries in that
                    # region, then stop and fall back to linear search
                    break

                # check whether this row is before or after the target location
                row = self._fetch_next()
                if row and row[self._loc_column] < target:
                    start = self._input_offset()
                else:
                    end = roffs

            self._set_input_pos(filenum, start)

            # final linear search
            row = self._fetch_next()
            while row and row[self._loc_column] < target:
                row = self._fetch_next()
            self._next_row = row

        except Error:
            raise
        except Exception as e:
            raise OperationalError(
                'unable to seek to %r in %s: %s' % (target, self.name, e))

    def _seek_indexed(self, column_number, target):
        try:
            for (filenum, f) in enumerate(self._table._files):
                indices = f[3]
                offs = indices[column_number].get(target, None)
                if offs is not None:
                    self._set_input_pos(filenum, offs)
                    self._next_row = self._fetch_next()
                    return
            self._seek_end()
        except Error:
            raise
        except Exception as e:
            raise OperationalError(
                'unable to seek to %r in %s: %s' % (target, self.name, e))

    def _seek_start(self):
        self._set_input_pos(0, 0)
        self._next_row = self._fetch_next()

    def _seek_end(self):
        self._infile = None
        self._infilenum = None
        self._inbuf = b''
        self._next_row = None

    def _set_input_pos(self, filenum, offset):
        self._infilenum = filenum
        if filenum < len(self._infiles):
            self._infile = self._infiles[filenum]
            self._infile.seek(offset)
        else:
            self._infile = None
        self._inbuf = b''
        self._next_row = None

    def _sync_input(self, filenum, offset):
        self._infilenum = filenum
        self._infile = self._infiles[filenum]
        self._infile.seek(offset)
        buf = self._infile.read(4096)
        m = self._table._sync_pattern.search(buf)
        while not m:
            b = self._infile.read(4096)
            if not b:
                return None
            buf += b
            m = self._table._sync_pattern.search(buf)
        i = m.start(self._table._sync_pattern_group)
        self._inbuf = buf[i:]
        return offset + i

    def _input_filename(self):
        try:
            return self._infile.name
        except Exception:
            return None

    def _input_offset(self):
        try:
            return self._infile.tell() - len(self._inbuf)
        except Exception:
            return None

    def _read_to_tab(self):
        """Read a binary string terminated by '\t'."""
        while b'\t' not in self._inbuf:
            self._read_ahead()
        (f, self._inbuf) = self._inbuf.split(b'\t', 1)
        return f

    def _read_to_lf(self):
        """Read a binary string terminated by '\n'."""
        while b'\n' not in self._inbuf:
            self._read_ahead()
        (f, self._inbuf) = self._inbuf.split(b'\n', 1)
        return f

    def _read_blob32(self):
        """Read a binary string with a 32-bit little-endian length prefix."""
        while len(self._inbuf) < 4:
            self._read_ahead()
        n = struct.unpack('<I', self._inbuf[0:4])[0]
        while len(self._inbuf) < 4 + n:
            self._read_ahead(n)
        f = self._inbuf[4:(4 + n)]
        self._inbuf = self._inbuf[(4 + n):]
        return f

    def _read_ahead(self, count = 4096):
        """Read the next chunk of data into the input buffer."""
        while self._infile:
            v = self._infile.read(count)
            if v:
                self._inbuf += v
                return True
            if self._inbuf:
                raise DataSyntaxError('unexpected EOF in %s'
                                      % self._infile.name)
            self._infilenum += 1
            if self._infilenum < len(self._infiles):
                self._infile = self._infiles[self._infilenum]
                self._infile.seek(0)
            else:
                self._infile = None
        raise EOFError()
