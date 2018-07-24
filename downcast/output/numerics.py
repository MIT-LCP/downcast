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

from ..messages import NumericValueMessage

_del_control = str.maketrans({x: ' ' for x in list(range(32)) + [127]})

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
        lbl = attr.sub_label.translate(_del_control)
        val = msg.value
        if val is None:
            val = ''
        logfile.append('%s\t%s' % (lbl, val))
        source.ack_message(chn, msg, self)

    def flush(self):
        self.archive.flush()

    def finalize_record(record):
        pass
