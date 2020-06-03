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

from datetime import datetime, timezone

from ..messages import EnumerationValueMessage

_del_control = str.maketrans({x: ' ' for x in list(range(32)) + [127]})

class EnumerationValueHandler:
    def __init__(self, archive):
        self.archive = archive
        self.last_event = {}

    def send_message(self, chn, msg, source, ttl):
        if not isinstance(msg, EnumerationValueMessage):
            return

        source.nack_message(chn, msg, self)

        # Load metadata for this numeric
        attr = msg.origin.get_enumeration_attr(msg.enumeration_id, (ttl <= 0))
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
        logfile = record.open_log_file('_phi_enums')

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

        # Write value to the log file
        lbl = attr.label.translate(_del_control)
        val = msg.value
        if val is None:
            val = ''
        else:
            val = val.translate(_del_control)
        logfile.append('%s\t%d\t%s' % (attr.label, attr.value_physio_id, val))
        source.ack_message(chn, msg, self)

    def flush(self):
        self.archive.flush()

class EnumerationValueFinalizer:
    def __init__(self, record):
        self.record = record
        self.log = record.open_log_reader('_phi_enums', allow_missing = True)

        # Scan the enums log file, and add timestamps to the time map
        for (sn, ts, line) in self.log.unsorted_items():
            ts = datetime.strptime(str(ts), '%Y%m%d%H%M%S%f')
            ts = ts.replace(tzinfo = timezone.utc)
            record.time_map.add_time(ts)

    def finalize_record(self):
        sn0 = self.record.seqnum0()

        if not self.log.missing():
            ef = self.record.open_log_file('enums')
            for (sn, ts, line) in self.log.sorted_items():
                if b'\030' in line:
                    continue
                ts = datetime.strptime(str(ts), '%Y%m%d%H%M%S%f')
                ts = ts.replace(tzinfo = timezone.utc)
                sn = self.record.time_map.get_seqnum(ts) or sn
                if sn0 is None:
                    sn0 = sn
                ef.fp.write(('%s\t' % (sn - sn0)).encode()) # XXX
                ef.fp.write(line.strip())                   # XXX
                ef.fp.write(b'\n')                          # XXX
