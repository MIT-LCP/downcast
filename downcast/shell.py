#
# dwcsql - simple interactive frontend for the DWC SQL database
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

import sys
import readline
import time
import os
import re
import locale
import ast
from argparse import ArgumentParser
from uuid import UUID
from decimal import Decimal

from .server import DWCDB
from .db.exceptions import ParameterCountError

################################################################

_known_tables = [
    'External_Alert',
    'External_BedTag',
    'External_Enumeration',
    'External_EnumerationValue',
    'External_Numeric',
    'External_NumericValue',
    'External_Patient',
    'External_PatientDateAttribute',
    'External_PatientStringAttribute',
    'External_Wave',
    'External_WaveSample',
    'Pdx_PartitionDetailView',
    '_Export.AlertArchive_',
    '_Export.Alert_',
    '_Export.BedTag_',
    '_Export.Configuration_',
    '_Export.DbMaintenanceLock_',
    '_Export.EnumerationValueArchive_',
    '_Export.EnumerationValue_',
    '_Export.Enumeration_',
    '_Export.NumericValueArchive_',
    '_Export.NumericValue_',
    '_Export.Numeric_',
    '_Export.PartitionSetting_',
    '_Export.PatientDateAttribute_',
    '_Export.PatientMappingArchive_',
    '_Export.PatientMapping_',
    '_Export.PatientStringAttribute_',
    '_Export.Patient_',
    '_Export.StorageLocation_',
    '_Export.WaveSampleArchive_',
    '_Export.WaveSample_',
    '_Export.Wave_'
]

_known_columns = [
    'AdmitState', 'AlertId', 'Alias', 'AnnounceTime', 'BasePhysioId',
    'BedLabel', 'CalibrationAbsLower', 'CalibrationAbsUpper',
    'CalibrationScaledLower', 'CalibrationScaledUpper',
    'CalibrationType', 'Category', 'Channel', 'ClinicalUnit', 'Code',
    'Color', 'CompoundValueId', 'EcgLeadPlacement', 'EndTime',
    'EnumerationId', 'Gender', 'Height', 'HeightUnit',
    'HighEdgeFrequency', 'Hostname', 'Id', 'InvalidSamples',
    'IsAlarmingOff', 'IsAperiodic', 'IsDerived', 'IsManual',
    'IsMapped', 'IsSilenced', 'IsSlowWave', 'IsTrendUploaded', 'Kind',
    'Label', 'LowEdgeFrequency', 'LowerLimit', 'MappingId',
    'MaxValues', 'Name', 'NumericId', 'OnsetTime', 'PacedMode',
    'PacedPulses', 'PatientId', 'PhysioId', 'PressureUnit',
    'ResuscitationStatus', 'SamplePeriod', 'Scale', 'ScaleLower',
    'ScaleUpper', 'SequenceNumber', 'Severity', 'Source', 'SubLabel',
    'SubPhysioId', 'SubtypeId', 'Tag', 'TimeStamp', 'Timestamp',
    'UnavailableSamples', 'UnitCode', 'UnitLabel', 'UpperLimit',
    'Validity', 'Value', 'ValuePhysioId', 'WaveId', 'WaveSamples',
    'Weight', 'WeightUnit'
]

_known_ids = {}

def _get_completions(text):
    for t in _known_tables:
        if t.startswith(text):
            yield t
    for c in _known_columns:
        if c.startswith(text):
            yield c
    if text.startswith("'"):
        prefix = text[1:3]
        if prefix in _known_ids:
            for z in _known_ids[prefix]:
                if z.startswith(text):
                    yield z

def _add_known_uuid(val):
    s = repr(str(val))
    prefix = s[1:3]
    if prefix not in _known_ids:
        _known_ids[prefix] = set()
    _known_ids[prefix].add(s)

_ctext = ''
_ccompl = []

def _completer(text, state):
    global _ctext, _ccompl
    if text != _ctext:
        _ctext = text
        _ccompl = sorted(_get_completions(text))
    if state < len(_ccompl):
        return _ccompl[state]
    else:
        return None

################################################################

_uuid_pattern = re.compile('\A[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z',
                           re.ASCII | re.IGNORECASE)

if sys.stdout.isatty() and os.environ.get('TERM', 'dumb') != 'dumb':
    _vcolor = ['\033[0m'] + ['\033[%dm' % i for i in range(31, 37)]
    _hcolor = ['\033[0;1m'] + ['\033[%d;1m' % i for i in range(31, 37)]
    _color0 = '\033[0m'
else:
    _vcolor = _hcolor = ['']
    _color0 = ''

_max_align_width = 64
_align_group_size = 20

def _format_value(val):
    if isinstance(val, bool):
        return repr(val)
    elif isinstance(val, Decimal) or isinstance(val, int):
        return '{:n}'.format(val)
    elif isinstance(val, UUID):
        _add_known_uuid(val)
        return repr(str(val))
    elif isinstance(val, str):
        if _uuid_pattern.match(val):
            _add_known_uuid(UUID(val))
        return repr(val)
    else:
        return repr(val)

def _value_alignment(val):
    return (isinstance(val, str)
            or isinstance(val, bytes)
            or isinstance(val, UUID))

def _pad(text, width, leftalign):
    if leftalign:
        return text.ljust(width)
    else:
        return text.rjust(width)

def _show_results(cur, colinfo, results, setindex):
    headers = (len(colinfo) == 0)
    if headers:
        for desc in cur.description:
            colinfo.append([len(desc[0]), None])
    table = []
    for row in results:
        while len(colinfo) < len(row):
            colinfo.append([0, None])
        tabrow = []
        for (i, value) in enumerate(row):
            text = _format_value(value)
            width = len(text)
            if width < _max_align_width:
                colinfo[i][0] = max(colinfo[i][0], width)
            if value is not None and colinfo[i][1] is None:
                colinfo[i][1] = _value_alignment(value)
            tabrow.append(text)
        table.append(tabrow)
    if headers:
        for (i, desc) in enumerate(cur.description):
            if i > 0:
                sys.stdout.write(' ')
            sys.stdout.write(_hcolor[(i + setindex) % len(_hcolor)])
            (width, leftalign) = colinfo[i]
            sys.stdout.write(_pad(desc[0], width, leftalign))
        sys.stdout.write(_color0 + '\n')
    for tabrow in table:
        for (i, text) in enumerate(tabrow):
            if i > 0:
                sys.stdout.write(' ')
            sys.stdout.write(_vcolor[(i + setindex) % len(_vcolor)])
            (width, leftalign) = colinfo[i]
            sys.stdout.write(_pad(text, width, leftalign))
        sys.stdout.write(_color0 + '\n')

def _run_query(db, query, params):
    if query is '':
        return
    with db.connect() as conn:
        with conn.cursor() as cur:
            begin = time.monotonic()
            cur.execute(query, params)

            more_results = True
            setindex = 0
            while more_results:
                colinfo = []
                headers = True
                results = []
                row = cur.fetchone()
                while row is not None:
                    results.append(row)
                    if len(results) >= _align_group_size:
                        _show_results(cur, colinfo, results, setindex)
                        results = []
                    row = cur.fetchone()
                _show_results(cur, colinfo, results, setindex)
                more_results = cur.nextset()
                setindex += 1
                if more_results:
                    print()

            end = time.monotonic()
            print('(%d rows; %.3f seconds)' % (cur.rowcount, end - begin))
            print()

################################################################

def main():
    locale.setlocale(locale.LC_ALL, '')

    p = ArgumentParser()
    p.add_argument('--server', metavar = 'NAME', default = 'demo')
    p.add_argument('--password-file', metavar = 'FILE',
                   default = 'server.conf')
    opts = p.parse_args()

    DWCDB.load_config(opts.password_file)

    db = DWCDB(opts.server)

    readline.set_completer_delims(' \t\n()[]=<>-+*?,')
    readline.parse_and_bind('tab: complete')
    readline.set_completer(_completer)

    histfile = os.environ.get('DWCSQL_HISTFILE', None)
    if histfile is not None:
        try:
            readline.read_history_file(histfile)
        except Exception:
            pass
    readline.set_history_length(1000)

    try:
        while True:
            try:
                line = input(opts.server + '> ')
                query = line
                while line is not '' and not query.endswith(';'):
                    line = input(' ' * len(opts.server) + '> ')
                    query += '\n' + line
                params = []
                while True:
                    try:
                        _run_query(db, query, params)
                        break
                    except ParameterCountError:
                        pass
                    line = input('? ')
                    params.append(ast.literal_eval(line.strip()))
            except KeyboardInterrupt:
                print()
            except EOFError:
                print()
                return
            except Exception as e:
                # nasty hack to extract the human-readable message from a
                # pymssql exception... is there a proper way to do this?
                if (hasattr(e, 'args') and isinstance(e.args, tuple)
                        and len(e.args) == 2 and isinstance(e.args[1], bytes)):
                    msg = e.args[1].decode('UTF-8', errors = 'replace')
                else:
                    msg = str(e)
                print('%s%s:\n%s\n' % (_color0, type(e).__name__, msg))
    finally:
        if histfile is not None:
            try:
                readline.write_history_file(histfile)
            except Exception:
                pass
