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
import datetime
import heapq
import os
import time

from .db import dwcbcp
from .messages import  bcp_format_description, bcp_format_message
from .parser import (AlertParser, EnumerationValueParser,
                     NumericValueParser, PatientMappingParser,
                     WaveSampleParser)
from .timestamp import T


def get_transition_time(date, timezone):
    """Check if the given date is a summer-to-winter transition date.

    date must be a datetime.date object, and timezone must be a TZ
    string, such as "EST5EDT,M3.2.0,M11.1.0" or
    "EST5EDT,M4.1.0,M10.5.0" (or "America/New_York" if you trust the
    system timezone database.)

    If, on that local date, the clock is set backward by an hour,
    return a 2-tuple:
    - a datetime.datetime corresponding to the transition point in
      local summer time
    - a datetime.datetime corresponding to the transition point in
      local winter time

    If the given local date is not a summer-to-winter transition date,
    return (None, None, None).

    >>> import datetime
    >>> (a, b) = get_transition_time(datetime.date(2004, 10, 31),
    ...                              "EST5EDT,M4.1.0,M10.5.0")
    >>> str(a)
    '2004-10-31 02:00:00-04:00'
    >>> str(b)
    '2004-10-31 01:00:00-05:00'

    """

    if timezone != os.environ.get('TZ'):
        os.environ['TZ'] = timezone
        time.tzset()

    prev_date = date - datetime.timedelta(days=1)
    next_date = date + datetime.timedelta(days=1)
    start_of_day = time.mktime((prev_date.year, prev_date.month, prev_date.day,
                                23, 59, 59, 0, 0, -1)) + 1
    end_of_day = time.mktime((next_date.year, next_date.month, next_date.day,
                              0, 0, 1, 0, 0, -1)) - 1

    if end_of_day - start_of_day == 24 * 60 * 60:
        # normal day - nothing to do
        return (None, None)
    elif end_of_day - start_of_day == 23 * 60 * 60:
        # forward (spring) transition day - nothing to do
        return (None, None)
    elif end_of_day - start_of_day == 25 * 60 * 60:
        # backward (fall) transition day
        summer_offs = []
        winter_offs = []
        summer_ldt = []
        winter_ldt = []
        for m in range(25 * 60):
            t = start_of_day + m * 60
            lt = time.localtime(t)
            ut = time.gmtime(t)
            ldt = datetime.datetime(lt.tm_year, lt.tm_mon, lt.tm_mday,
                                    lt.tm_hour, lt.tm_min, lt.tm_sec)
            udt = datetime.datetime(ut.tm_year, ut.tm_mon, ut.tm_mday,
                                    ut.tm_hour, ut.tm_min, ut.tm_sec)
            offs = (ldt - udt).total_seconds()
            if lt.tm_isdst == 0:
                winter_offs.append(offs)
                winter_ldt.append(ldt)
            elif lt.tm_isdst == 1:
                summer_offs.append(offs)
                summer_ldt.append(ldt)
            else:
                raise NotImplementedError('cannot make sense of timezone')

        if (winter_ldt[0] - summer_ldt[-1] == datetime.timedelta(minutes=-59)
                and min(summer_offs) == max(summer_offs)
                and min(winter_offs) == max(winter_offs)
                and min(summer_offs) == min(winter_offs) + 60 * 60):
            summer_offs = datetime.timedelta(seconds=summer_offs[0])
            summer_tz = datetime.timezone(summer_offs)
            winter_offs = datetime.timedelta(seconds=winter_offs[0])
            winter_tz = datetime.timezone(winter_offs)
            winter_tt = winter_ldt[0].replace(tzinfo=winter_tz)
            summer_tt = winter_tt.astimezone(summer_tz)
            return (summer_tt, winter_tt)
        else:
            raise NotImplementedError('cannot make sense of timezone')
    else:
        raise NotImplementedError('cannot make sense of timezone')


def read_mapping_timestamps(mapping_files, out_timezone):
    """Read timestamps from one or more PatientMapping files.

    The result is a dictionary that maps a MappingId to the timestamp
    of the patient mapping.  All timestamps are translated to
    out_timezone for convenience.
    """
    mapping_timestamps = {}

    for data_file in mapping_files:
        table_abbr, _ = os.path.splitext(os.path.basename(data_file))
        table = '_Export.%s_' % table_abbr
        format_file = os.path.join(os.path.dirname(data_file),
                                   table_abbr + '.fmt')

        db = dwcbcp.DWCBCPConnection([])
        db.add_data_file(table, data_file, format_file)
        cursor = db.cursor()
        parser = PatientMappingParser(limit = None, dialect = 'sqlite',
                                      paramstyle = dwcbcp.paramstyle)
        message_iter = parser.parse(origin = None, cursor = cursor)
        n = 0
        for message in message_iter:
            t = message.timestamp.astimezone(out_timezone)
            mapping_timestamps[message.mapping_id] = t
            n += 1

    return mapping_timestamps


def fixup_bcp_file(table_abbr, input_data_file, output_data_file,
                   mapping_files, timezone):
    """Read a BCP file and fix incorrect timestamps in it.

    table_abbr must be one of the following:
    - 'Alert'
    - 'EnumerationValue'
    - 'NumericValue'
    - 'WaveSample'

    (Timestamps in PatientMapping do not require fixups, and we have
    no ability to assess whether timestamps in
    Patient/PatientDateAttribute/PatientStringAttribute are correct or
    not.)

    input_data_file is the path to the input data file.  An
    accompanying .fmt file must exist in the same directory.

    mapping_files is a sequence of corresponding PatientMapping files.
    Accompanying .fmt files must exist in the same directory.

    timezone must be a TZ string and defines the timezone rules that
    are presumed to have been configured (and applied incorrectly)
    within the DWC system.
    """

    parser_types = {
        'Alert': AlertParser,
        'EnumerationValue': EnumerationValueParser,
        'NumericValue': NumericValueParser,
        'WaveSample': WaveSampleParser,
    }
    parser_type = parser_types[table_abbr]
    table = '_Export.%s_' % table_abbr

    format_file = os.path.join(os.path.dirname(input_data_file),
                               table_abbr + '.fmt')

    db = dwcbcp.DWCBCPConnection([])
    db.add_data_file(table, input_data_file, format_file)

    cursor = db.cursor()
    parser = parser_type(limit = None, dialect = 'sqlite',
                         paramstyle = dwcbcp.paramstyle)
    for message in parser.parse(origin = None, cursor = cursor):
        dump_date = (message.timestamp + datetime.timedelta(hours=12)).date()
        break

    prev_date = dump_date - datetime.timedelta(days=1)
    next_date = dump_date + datetime.timedelta(days=1)
    for d in (dump_date, next_date, prev_date):
        (summer_tt, winter_tt) = get_transition_time(d, timezone)
        if summer_tt is not None:
            break
    else:
        return False

    # Incorrectly-labelled timestamps have:
    #  - time zone set to winter_tz (when it should be summer_tz)
    #  - timestamp between winter_tt and (winter_tt + 1 hour)
    #
    # For example, an event that actually occurred at 1:30 summer time
    # would be incorrectly labelled as 1:30 winter time.
    #
    # Therefore, events that occur before winter_tt are assumed to be
    # correct and don't need to be touched.  Events that occur after
    # (winter_tt + 65 minutes) are assumed to be correct and don't
    # need to be touched.
    #
    # Events that occur between these two timestamps will be
    # corrected if they are labelled as winter_tz and if they occur
    # more than 30 minutes after the corresponding patient mapping
    # time.
    #
    # (Note that patient mapping timestamps do not suffer from the
    # incorrect labelling issue; indeed, those timestamps are derived
    # from the clock on the export host, so if we set the system
    # timezone to Iceland then all of those timestamps will be UTC.)
    #
    # When correcting a timestamp, its timezone is changed to one hour
    # past summer time; for example, a timestamp originally stored as
    # "01:30:00 -05:00", which really occurred at "01:30:00 -04:00",
    # is written as "02:30:00 -03:00", so that it's later possible to
    # identify corrected timestamps if necessary.

    # I would rather add 5 minutes of leeway here, but that would mean
    # correcting an event timestamp into the *previous day* (because
    # we always dump starting at midnight -05:00), and shuffling
    # messages between dumps would just be a nightmare.
    ambiguous_start = winter_tt
    ambiguous_end = winter_tt + datetime.timedelta(minutes = 65)

    summer_tz = summer_tt.tzinfo
    winter_tz = winter_tt.tzinfo
    summer_offset = summer_tz.utcoffset(summer_tt)
    winter_offset = winter_tz.utcoffset(winter_tt)
    dt_30_minutes = datetime.timedelta(minutes = 30)
    dt_60_minutes = datetime.timedelta(minutes = 60)
    corrected_tz = datetime.timezone(summer_offset + dt_60_minutes)

    overlap_start = ambiguous_start - dt_60_minutes
    overlap_end = ambiguous_end

    mapping_timestamps = read_mapping_timestamps(mapping_files, winter_tz)

    totals = [0, 0, 0]

    def fix_timestamp(mapping_id, timestamp, counter_inc=1):
        if not ambiguous_start <= timestamp < ambiguous_end:
            return timestamp
        try:
            mts = mapping_timestamps[mapping_id]
        except KeyError:
            raise Exception(
                'Unknown mapping ID %r at time %r.  '
                'Do you need to specify additional PatientMapping files?'
                % (mapping_id, timestamp))
        if timestamp.tzinfo != winter_tz:
            totals[0] += counter_inc
            return timestamp
        if timestamp - mts <= dt_30_minutes:
            totals[1] += counter_inc
            return timestamp
        totals[2] += counter_inc
        return T((timestamp - dt_60_minutes).astimezone(corrected_tz))

    def get_raw_messages(start, end):
        parser = parser_type(limit = None,
                             time_ge = start,
                             time_lt = end,
                             dialect = 'sqlite',
                             paramstyle = dwcbcp.paramstyle)
        return parser.parse(origin = None, cursor = db.cursor())

    if table_abbr == 'Alert':
        def get_messages(start, end):
            for message in get_raw_messages(start, end):
                yield message._replace(
                    onset_time = fix_timestamp(message.mapping_id,
                                               message.onset_time),
                    announce_time = fix_timestamp(message.mapping_id,
                                                  message.announce_time),
                    end_time = fix_timestamp(message.mapping_id,
                                             message.end_time))
    else:
        get_messages = get_raw_messages

    def corrected_messages():
        for message in get_messages(overlap_start, overlap_end):
            fixed_ts = fix_timestamp(message.mapping_id, message.timestamp)
            if fixed_ts is not message.timestamp:
                yield (fixed_ts, message._replace(timestamp=fixed_ts))

    def uncorrected_messages():
        for message in get_messages(overlap_start, overlap_end):
            fixed_ts = fix_timestamp(message.mapping_id, message.timestamp, 0)
            if fixed_ts is message.timestamp:
                yield (message.timestamp, message)

    iter_1 = get_messages(None, overlap_start)
    iter_2a = corrected_messages()
    iter_2b = uncorrected_messages()
    iter_2 = heapq.merge(iter_2a, iter_2b)
    iter_3 = get_messages(overlap_end, None)

    print('%s' % os.path.basename(input_data_file))
    print(' Fixing timestamps from: %s' % ambiguous_start)
    print('                     to: %s' % ambiguous_end)

    with open(output_data_file, 'wb') as outf:
        for message in iter_1:
            outf.write(bcp_format_message(message))
        for _, message in iter_2:
            outf.write(bcp_format_message(message))
        for message in iter_3:
            outf.write(bcp_format_message(message))

    output_table_abbr, _ = os.path.splitext(os.path.basename(output_data_file))
    output_format_file = os.path.join(os.path.dirname(output_data_file),
                                      output_table_abbr + '.fmt')
    with open(output_format_file, 'w') as fmtf:
        fmtf.write(bcp_format_description(message))

    print(' %8d timestamps not marked as winter time'
          % totals[0])
    print(' %8d timestamps correctly marked as winter time'
          % totals[1])
    print(' %8d timestamps incorrectly marked, changed to summer time'
          % totals[2])
    return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument('-z', '--timezone',
                   metavar = 'ZONE', required = True)
    p.add_argument('-m', '--mapping-file',
                   metavar = 'PatientMapping.dat', nargs = '+')
    p.add_argument('-o', '--output-file',
                   metavar = 'OUTPUT.dat', required = True)
    p.add_argument('input_file', metavar = 'INPUT.dat')
    opts = p.parse_args()

    table_abbr, _ = os.path.splitext(os.path.basename(opts.input_file))
    fixup_bcp_file(table_abbr, opts.input_file, opts.output_file,
                   opts.mapping_file, opts.timezone)
