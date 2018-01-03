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

class NumericValueHandler:
    def __init__(self, archive):
        self.archive = archive
        self.files = set()

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

        # Look up the corresponding record and add event to the time map
        record = self.archive.get_record(msg, (ttl <= 0))
        if record is None:
            # Record not yet available - hold message in pending and
            # continue processing
            return

        # Open or create a log file
        logfile = record.open_log_file('_numerics')
        self.files.add(logfile)

        # Write value to the log file
        time = msg.sequence_number - record.seqnum0()
        val = msg.value
        logfile.append('%d,%s,%s' % (time, attr.sub_label, val))
        source.ack_message(chn, msg, self)

    def flush(self):
        for f in self.files:
            f.flush()
        self.files = set()
        self.archive.flush()
