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

from ..messages import EnumerationValueMessage
from ..timestamp import delta_ms

class EnumerationValueHandler:
    def __init__(self, archive):
        self.archive = archive
        self.files = set()

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
        record = self.archive.get_record(msg, (ttl <= 0))
        if record is None:
            # Record not yet available - hold message in pending and
            # continue processing
            return

        # Determine the wall clock time of the corresponding waveform
        # message
        wtime = record.get_clock_time(msg.sequence_number, (ttl <= 0))
        if wtime is None and ttl > 0:
            # Timing information not yet available - hold message in
            # pending and continue processing
            return
        elif wtime is None:
            # FIXME: add something to indicate that the event
            # timestamp is not accurate
            wtime = msg.timestamp

        # Open or create a log file
        logfile = record.open_log_file('_enums')
        self.files.add(logfile)

        # Write value to the log file
        time = (msg.sequence_number + delta_ms(msg.timestamp, wtime)
                - record.seqnum0())
        val = msg.value
        logfile.append('%d,%s,%d,%s' % (time, attr.label,
                                        attr.value_physio_id, val))
        source.ack_message(chn, msg, self)

    def flush(self):
        for f in self.files:
            f.flush()
        self.files = set()
        self.archive.flush()
