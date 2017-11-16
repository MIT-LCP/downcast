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
        self.wave_attr = {}
        self.numeric_attr = {}
        self.enumeration_attr = {}
        self.attr_db = None

    def __repr__(self):
        return ('%s(%r)' % (self.__class__.__name__, self.servername))

    def connect(self):
        return pymssql.connect(self.hostname, self.username,
                               self.password, self.database)

    def dialect(self):
        return 'ms'

    def paramstyle(self):
        return pymssql.paramstyle

    def get_wave_attr(self, wave_id, sync):
        if wave_id in self.wave_attr:
            return self.wave_attr[wave_id]

        p = WaveAttrParser(dialect = self.dialect(),
                           paramstyle = self.paramstyle(),
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
                              paramstyle = self.paramstyle(),
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
                                  paramstyle = self.paramstyle(),
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
        # FIXME: add asynchronous processing
        try:
            if self.attr_db is None:
                self.attr_db = self.connect()

            (query, handler) = next(parser.queries)
            c = self.attr_db.cursor()
            c.query(*query)
            results = []
            row = c.read_row()
            while row is not None:
                results.append(handler(row))
                row = c.read_row()
            c.close()

            if len(results) > 1:
                self._log_warning('multiple results found for %r' % parser)

            return results[0]
        except Exception:
            raise UnknownAttrError()

class UnknownAttrError(Exception):
    """Internal exception indicating the object does not exist."""
    pass

class UnavailableAttrError(Exception):
    """Internal exception indicating that the request is pending."""
    pass
