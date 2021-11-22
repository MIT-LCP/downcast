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
import heapq

from ..messages import NumericValueMessage
from ..util import string_to_ascii

class NumericValueHandler:
    def __init__(self, archive):
        self.archive = archive
        self.last_periodic = {}
        self.last_aperiodic = {}

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

        # Dump original message to BCP file if desired
        if record.dump(msg):
            source.ack_message(chn, msg, self)
            return

        if attr.is_aperiodic:
            # Open or create a log file
            logfile = record.open_log_file('_phi_aperiodics')

            # Write the sequence number to the log file
            # (if it doesn't differ from the previous event)
            sn = msg.sequence_number
            old_sn = self.last_aperiodic.get(record, None)
            if sn != old_sn:
                logfile.append('S%s' % sn)
            self.last_aperiodic[record] = sn

            # Write the value to the log file
            lbl = string_to_ascii(attr.sub_label)
            ulbl = string_to_ascii(attr.unit_label)
            val = msg.value
            if val is None:
                val = ''
            logfile.append('%s\t%s\t%s' % (lbl, val, ulbl))
            source.ack_message(chn, msg, self)

        else:
            # Open or create a log file
            logfile = record.open_log_file('_phi_numerics')

            # Write the sequence number and timestamp to the log file
            # (if they don't differ from the previous event)
            sn = msg.sequence_number
            ts = msg.timestamp
            (old_sn, old_ts) = self.last_periodic.get(record, (None, None))
            if sn != old_sn:
                logfile.append('S%s' % sn)
            if ts != old_ts:
                logfile.append(ts.strftime_utc('%Y%m%d%H%M%S%f'))
            self.last_periodic[record] = (sn, ts)

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

        # Scan the log files; make a list of all non-null
        # numerics, and add timestamps to the time map
        self.all_numerics = set()

        self.periodic_log = record.open_log_reader('_phi_numerics',
                                                   allow_missing = True)
        for (sn, ts, line) in self.periodic_log.unsorted_items():
            ts = datetime.strptime(str(ts), '%Y%m%d%H%M%S%f')
            ts = ts.replace(tzinfo = timezone.utc)
            record.time_map.add_time(ts)
            if b'\030' not in line:
                parts = line.rstrip(b'\n').split(b'\t')
                # ignore nulls
                if len(parts) >= 3 and parts[1]:
                    self.all_numerics.add((parts[0], parts[2]))

        self.aperiodic_log = record.open_log_reader('_phi_aperiodics',
                                                    allow_missing = True)
        for (sn, _, line) in self.aperiodic_log.unsorted_items():
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
            for (sn, ts, line) in heapq.merge(
                    self.periodic_log.sorted_items(),
                    self.aperiodic_log.sorted_items()):
                if b'\030' in line:
                    continue
                parts = line.rstrip(b'\n').split(b'\t')
                # ignore nulls
                if len(parts) < 3 or not parts[1]:
                    continue
                col_id = (parts[0], parts[2])

                # determine new time value
                if ts == cur_ts and sn == cur_sn:
                    time = cur_time
                else:
                    if ts == 0:
                        # for aperiodics (such as NBP), use sequence number as
                        # observation time
                        obs_sn = sn
                    else:
                        # for periodics, translate timestamp to
                        # sequence number and use that as observation
                        # time
                        ts = datetime.strptime(str(ts), '%Y%m%d%H%M%S%f')
                        ts = ts.replace(tzinfo = timezone.utc)
                        obs_sn = self.record.time_map.get_seqnum(ts, sn + 5120)
                        if obs_sn is None:
                            obs_sn = sn

                    if sn0 is None:
                        sn0 = obs_sn
                    # Time measured in counter ticks, ick.
                    # Better would probably be to use (real) seconds
                    time = str(obs_sn - sn0).encode()
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
