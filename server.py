#
# downcast - tools for unpacking patient data from DWC
#
# Copyright (c) 2017 Laboratory for Computational Physiology
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

from configparser import ConfigParser
import pymssql

from parser import (WaveAttrParser, NumericAttrParser,
                    EnumerationAttrParser)
from attributes import (undefined_wave, undefined_numeric,
                        undefined_enumeration)

class DWCDB:
    _config = None

    def load_config(filename):
        DWCDB._config = ConfigParser()
        DWCDB._config.read(filename)

    def __init__(self, servername):
        self.servername = servername
        self.hostname = DWCDB._config[servername]['hostname']
        self.username = DWCDB._config[servername]['username']
        self.password = DWCDB._config[servername]['password']
        self.database = DWCDB._config[servername]['database']
        self.dialect = 'ms'
        self.paramstyle = pymssql.paramstyle
        self.wave_attr = {}
        self.numeric_attr = {}
        self.enumeration_attr = {}
        self.attr_db = None

    def __repr__(self):
        return ('%s(%r)' % (self.__class__.__name__, self.servername))

    def connect(self):
        return pymssql.connect(self.hostname, self.username,
                               self.password, self.database)

    def get_wave_attr(self, wave_id, sync):
        if wave_id in self.wave_attr:
            return self.wave_attr[wave_id]

        p = WaveAttrParser(dialect = self.dialect,
                           paramstyle = self.paramstyle,
                           limit = 2, wave_id = wave_id)
        try:
            v = self._parse_attr(p, sync)
        except UnknownAttrError:
            v = undefined_wave
        except UnavailableAttrError:
            return None
        self.wave_attr[wave_id] = v
        return v

    def get_numeric_attr(self, numeric_id, sync):
        if numeric_id in self.numeric_attr:
            return self.numeric_attr[numeric_id]

        p = NumericAttrParser(dialect = self.dialect,
                              paramstyle = self.paramstyle,
                              limit = 2, numeric_id = numeric_id)
        try:
            v = self._parse_attr(p, sync)
        except UnknownAttrError:
            v = undefined_numeric
        except UnavailableAttrError:
            return None
        self.numeric_attr[numeric_id] = v
        return v

    def get_enumeration_attr(self, enumeration_id, sync):
        if enumeration_id in self.enumeration_attr:
            return self.enumeration_attr[enumeration_id]

        p = EnumerationAttrParser(dialect = self.dialect,
                                  paramstyle = self.paramstyle,
                                  limit = 2, enumeration_id = enumeration_id)
        try:
            v = self._parse_attr(p, sync)
        except UnknownAttrError:
            v = undefined_enumeration
        except UnavailableAttrError:
            return None
        self.enumeration_attr[enumeration_id] = v
        return v

    def _parse_attr(self, parser, sync):
        if self.attr_db is None:
            self.attr_db = self.connect()

        # FIXME: add asynchronous processing
        try:
            cursor = self.attr_db.cursor()
            results = []
            for (query, handler) in parser.queries():
                cursor.execute(*query)
                row = cursor.fetchone()
                while row is not None:
                    results.append(handler(self, row))
                    row = cursor.fetchone()
            if len(results) > 1:
                self._log_warning('multiple results found for %r' % parser)
            elif len(results) == 0:
                raise UnknownAttrError()
            return results[0]
        finally:
            cursor.close()

class UnknownAttrError(Exception):
    """Internal exception indicating the object does not exist."""
    pass

class UnavailableAttrError(Exception):
    """Internal exception indicating that the request is pending."""
    pass
