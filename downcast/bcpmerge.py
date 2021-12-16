#
# downcast - tools for unpacking patient data from DWC
#
# Copyright (c) 2021 Laboratory for Computational Physiology
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

import argparse
import heapq
import os

from .db import dwcbcp
from .messages import bcp_format_description, bcp_format_message
from .parser import (AlertParser, BedTagParser, EnumerationValueParser,
                     NumericValueParser, PatientBasicInfoParser,
                     PatientDateAttributeParser, PatientMappingParser,
                     PatientStringAttributeParser, WaveSampleParser)
from .timestamp import T

def merge_files(table_abbr, input_files, output_data_file,
                output_format_file = None, start = None, end = None):
    parser_types = {
        'Alert': AlertParser,
        'BedTag': BedTagParser,
        'EnumerationValue': EnumerationValueParser,
        'NumericValue': NumericValueParser,
        'Patient': PatientBasicInfoParser,
        'PatientDateAttribute': PatientDateAttributeParser,
        'PatientMapping': PatientMappingParser,
        'PatientStringAttribute': PatientStringAttributeParser,
        'WaveSample': WaveSampleParser,
    }
    parser_type = parser_types[table_abbr]
    table = '_Export.%s_' % table_abbr

    input_files = list(input_files)
    dbs = []
    cursors = []
    message_iters = []
    for (data_file, format_file) in input_files:
        db = dwcbcp.DWCBCPConnection([])
        db.add_data_file(table, data_file, format_file)
        dbs.append(db)
        cursor = db.cursor()
        cursors.append(cursor)
        parser = parser_type(limit = None, dialect = 'sqlite',
                             paramstyle = dwcbcp.paramstyle,
                             time_ge = start, time_lt = end)
        message_iter = parser.parse(origin = None, cursor = cursor)
        message_iters.append(message_iter)

    with open(output_data_file, 'wb') as outf:
        for message in heapq.merge(*message_iters,
                                   key = lambda x: x.timestamp):
            outf.write(bcp_format_message(message))

    if output_format_file is not None:
        with open(output_format_file, 'w') as fmtf:
            fmtf.write(bcp_format_description(message))

def _parse_timestamp(arg):
    try:
        return T(arg)
    except Exception:
        raise ArgumentTypeError(
            "%r is not in the format 'YYYY-MM-DD HH:MM:SS.SSS +ZZ:ZZ'" % arg)

def main():
    p = argparse.ArgumentParser()
    p.add_argument('-t', '--table', metavar = 'TABLE')
    p.add_argument('-f', '--format-file',
                   metavar = 'TABLE.fmt', required = True)
    p.add_argument('-o', '--output-file',
                   metavar = 'OUTPUT.dat', required = True)
    p.add_argument('--start', metavar = 'TIME', type = _parse_timestamp)
    p.add_argument('--end', metavar = 'TIME', type = _parse_timestamp)
    p.add_argument('input_files', metavar = 'INPUT.dat', nargs = '+')
    opts = p.parse_args()

    table_abbr = opts.table
    if table_abbr is None:
        table_abbr, _ = os.path.splitext(os.path.basename(opts.format_file))

    input_files = [(f, opts.format_file) for f in opts.input_files]

    output_table_abbr, _ = os.path.splitext(os.path.basename(opts.output_file))
    output_format_file = os.path.join(os.path.dirname(opts.output_file),
                                      output_table_abbr + '.fmt')

    merge_files(table_abbr, input_files, opts.output_file,
                output_format_file = output_format_file,
                start = opts.start, end = opts.end)
