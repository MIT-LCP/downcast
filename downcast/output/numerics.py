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

from datetime import datetime, timezone

from ..messages import NumericValueMessage
from ..util import string_to_ascii

class NumericValueHandler:
    def __init__(self, archive):
        self.archive = archive
        self.last_event = {}

    def send_message(self, chn, msg, source, ttl):
        if not isinstance(msg, NumericValueMessage):
            return

        source.nack_message(chn, msg, self)

        # Load metadata for this numeric
        attr = msg.origin.get_numeric_attr(msg.numeric_id, (ttl <= 0))
        if attr is None:
            # Metadata not yet available - hold message in pending and
            # continue processing
            return

        # Look up the corresponding record
        record = self.archive.get_record(msg)
        if record is None:
            # Record not yet available - hold message in pending and
            # continue processing
            return

        # Open or create a log file
        logfile = record.open_log_file('_phi_numerics')

        # Write the sequence number and timestamp to the log file
        # (if they don't differ from the previous event)
        sn = msg.sequence_number
        ts = msg.timestamp
        (old_sn, old_ts) = self.last_event.get(record, (None, None))
        if sn != old_sn:
            logfile.append('S%s' % sn)
        if ts != old_ts:
            logfile.append(ts.strftime_utc('%Y%m%d%H%M%S%f'))
        self.last_event[record] = (sn, ts)

        # Write the value to the log file
        lbl = string_to_ascii(attr.sub_label)
        ulbl = string_to_ascii(attr.unit_label)
        val = msg.value
        if val is None:
            val = ''
        logfile.append('%s\t%s\t%s' % (lbl, val, ulbl))
        source.ack_message(chn, msg, self)

    def flush(self):
        self.archive.flush()

class NumericValueFinalizer:
    def __init__(self, record):
        self.record = record
        self.log = record.open_log_reader('_phi_numerics',
                                          allow_missing = True)

        # Scan the numerics log file; make a list of all non-null
        # numerics, and add timestamps to the time map
        self.all_numerics = set()
        for (sn, ts, line) in self.log.unsorted_items():
            ts = datetime.strptime(str(ts), '%Y%m%d%H%M%S%f')
            ts = ts.replace(tzinfo = timezone.utc)
            record.time_map.add_time(ts)
            if b'\030' not in line:
                parts = line.rstrip(b'\n').split(b'\t')
                # ignore nulls
                if len(parts) >= 3 and parts[1]:
                    self.all_numerics.add((parts[0], parts[2]))

    def finalize_record(self):
        sn0 = self.record.seqnum0()

        if self.all_numerics:
            num_columns = sorted(self.all_numerics)
            num_index = {n: i + 1 for i, n in enumerate(num_columns)}

            nf = self.record.open_log_file('numerics.csv', truncate = True)
            row = [b'"time"']
            for (name, units) in num_columns:
                desc = name + b' [' + (units or b'NU') + b']'
                row.append(b'"' + desc.replace(b'"', b'""') + b'"')
            cur_ts = None
            cur_sn = None
            cur_time = None
            for (sn, ts, line) in self.log.sorted_items():
                if b'\030' in line:
                    continue
                parts = line.rstrip(b'\n').split(b'\t')
                # ignore nulls
                if len(parts) < 3 or not parts[1]:
                    continue
                col_id = (parts[0], parts[2])

                # determine new time value
                if ts == cur_ts and sn == cur_sn:
                    time = cur_values[0]
                else:
                    ts = datetime.strptime(str(ts), '%Y%m%d%H%M%S%f')
                    ts = ts.replace(tzinfo = timezone.utc)
                    sn = self.record.time_map.get_seqnum(ts, sn + 5120) or sn
                    if sn0 is None:
                        sn0 = sn
                    # Time measured in counter ticks, ick.
                    # Better would probably be to use (real) seconds
                    time = str(sn - sn0).encode()
                    cur_ts = ts
                    cur_sn = sn
                    cur_time = time

                # write out a complete row if the time value has changed
                if time != row[0]:
                    nf.fp.write(b','.join(row))
                    nf.fp.write(b'\n')
                    row = [time] + [b''] * len(self.all_numerics)
                row[num_index[col_id]] = parts[1].rstrip(b'0').rstrip(b'.')
            # write the final row
            nf.fp.write(b','.join(row))
            nf.fp.write(b'\n')
