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

from configparser import ConfigParser
import pymssql
import sqlite3
import warnings
import os

from .parser import (WaveAttrParser, NumericAttrParser,
                     EnumerationAttrParser, PatientMappingParser,
                     DBSyntaxError)
from .attributes import (undefined_wave, undefined_numeric,
                         undefined_enumeration)

class DWCDB:
    _config = None

    def load_config(filename):
        DWCDB._config = ConfigParser()
        DWCDB._config.read(filename)

    def __init__(self, servername):
        self._server = DWCDBServer.get(servername)
        self.servername = servername
        self.dialect = self._server.dialect
        self.paramstyle = self._server.paramstyle

    def __repr__(self):
        return ('%s(%r)' % (self.__class__.__name__, self.servername))

    def __getstate__(self):
        return self.servername

    def __setstate__(self, servername):
        DWCDB.__init__(self, servername)

    def connect(self):
        return self._server.connect()

    def get_messages(self, parser, connection = None, cursor = None):
        tmpconn = None
        tmpcur = None
        try:
            if cursor is not None:
                cur = cursor
            elif connection is not None:
                cur = tmpcur = connection.cursor()
            else:
                tmpconn = self._server.connect()
                cur = tmpcur = tmpconn.cursor()
            for (query, handler) in parser.queries():
                cur.execute(*query)
                row = cur.fetchone()
                while row is not None:
                    msg = handler(self, row)
                    if msg is not None:
                        yield msg
                    row = cur.fetchone()
        finally:
            if tmpcur is not None:
                tmpcur.close()
            if tmpconn is not None:
                tmpconn.close()

    def get_wave_attr(self, wave_id, sync):
        v = self._server.wave_attr.get(wave_id, None)
        if v is not None:
            return v

        p = WaveAttrParser(dialect = self.dialect,
                           paramstyle = self.paramstyle,
                           limit = 2, wave_id = wave_id)
        try:
            v = self._parse_attr(p, sync)
        except UnknownAttrError:
            v = undefined_wave
        except DBSyntaxError as e:
            warnings.warn(e.warning(), stacklevel = 2)
            v = undefined_wave
        except UnavailableAttrError:
            return None
        self._server.wave_attr[wave_id] = v
        return v

    def get_numeric_attr(self, numeric_id, sync):
        v = self._server.numeric_attr.get(numeric_id, None)
        if v is not None:
            return v

        p = NumericAttrParser(dialect = self.dialect,
                              paramstyle = self.paramstyle,
                              limit = 2, numeric_id = numeric_id)
        try:
            v = self._parse_attr(p, sync)
        except UnknownAttrError:
            v = undefined_numeric
        except DBSyntaxError as e:
            warnings.warn(e.warning(), stacklevel = 2)
            v = undefined_numeric
        except UnavailableAttrError:
            return None
        self._server.numeric_attr[numeric_id] = v
        return v

    def get_enumeration_attr(self, enumeration_id, sync):
        v = self._server.enumeration_attr.get(enumeration_id, None)
        if v is not None:
            return v

        p = EnumerationAttrParser(dialect = self.dialect,
                                  paramstyle = self.paramstyle,
                                  limit = 2, enumeration_id = enumeration_id)
        try:
            v = self._parse_attr(p, sync)
        except UnknownAttrError:
            v = undefined_enumeration
        except DBSyntaxError as e:
            warnings.warn(e.warning(), stacklevel = 2)
            v = undefined_enumeration
        except UnavailableAttrError:
            return None
        self._server.enumeration_attr[enumeration_id] = v
        return v

    def get_patient_id(self, mapping_id, sync):
        v = self._server.patient_map.get(mapping_id, None)
        if v is not None:
            return v
        # if not sync:
        #     return None

        p = PatientMappingParser(dialect = self.dialect,
                                 paramstyle = self.paramstyle,
                                 limit = 2, mapping_id = mapping_id)
        try:
            v = self._parse_attr(p, True)
        except UnknownAttrError:
            return None
        except DBSyntaxError as e:
            warnings.warn(e.warning(), stacklevel = 2)
            self._server.patient_map[mapping_id] = None
            return None
        self.set_patient_id(mapping_id, v.patient_id)
        return v.patient_id

    def set_patient_id(self, mapping_id, patient_id):
        self._server.patient_map[mapping_id] = patient_id

    def _parse_attr(self, parser, sync):
        # ensure that attr_db connections are not shared between
        # processes
        pid = os.getpid()
        if self._server.attr_db_pid == pid:
            conn = self._server.attr_db
        else:
            self._server.attr_db = conn = self._server.connect()
            self._server.attr_db_pid = pid

        # FIXME: add asynchronous processing
        results = []
        for msg in self.get_messages(parser, connection = conn):
            results.append(msg)
        if len(results) > 1:
            self._log_warning('multiple results found for %r' % parser)
        elif len(results) == 0:
            raise UnknownAttrError()
        return results[0]

class DWCDBServer:
    _named_servers = {}

    def __init__(self, servername):
        self.dbtype = DWCDB._config.get(servername, 'type', fallback = 'mssql')

        if self.dbtype == 'mssql':
            self.hostname = DWCDB._config[servername]['hostname']
            self.username = DWCDB._config[servername]['username']
            self.password = DWCDB._config[servername]['password']
            self.database = DWCDB._config[servername]['database']
            self.dialect = 'ms'
            self.paramstyle = pymssql.paramstyle
        elif self.dbtype == 'sqlite':
            self.filename = DWCDB._config[servername]['file']
            self.dialect = 'sqlite'
            self.paramstyle = sqlite3.paramstyle
        else:
            raise ValueError('unknown database type')

        self.wave_attr = {}
        self.numeric_attr = {}
        self.enumeration_attr = {}
        self.patient_map = {}
        self.attr_db = None
        self.attr_db_pid = None

    def get(servername):
        s = DWCDBServer._named_servers.get(servername, None)
        if s is None:
            s = DWCDBServer(servername)
            DWCDBServer._named_servers[servername] = s
        return s

    def connect(self):
        if self.dbtype == 'mssql':
            return pymssql.connect(self.hostname, self.username,
                                   self.password, self.database)
        elif self.dbtype == 'sqlite':
            return sqlite3.connect(self.filename)

class UnknownAttrError(Exception):
    """Internal exception indicating the object does not exist."""
    pass

class UnavailableAttrError(Exception):
    """Internal exception indicating that the request is pending."""
    pass
