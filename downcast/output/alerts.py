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

from ..messages import AlertMessage
from ..timestamp import (T, delta_ms)

_sane_time = T('1970-01-01 00:00:00.000 +00:00')

class AlertHandler:
    def __init__(self, archive):
        self.archive = archive

    def send_message(self, chn, msg, source, ttl):
        if not isinstance(msg, AlertMessage):
            return

        source.nack_message(chn, msg, self)

        # Look up the corresponding record
        record = self.archive.get_record(msg, (ttl <= 0))
        if record is None:
            # Record not yet available - hold message in pending and
            # continue processing
            return

        # Open or create a log file
        logfile = record.open_log_file('_phi_alerts')

        # Write value to the log file
        sn = msg.sequence_number
        ts = msg.timestamp.strftime_utc('%Y%m%d%H%M%S%f')
        ats = ots = ets = None
        if msg.announce_time and msg.announce_time > _sane_time:
            ats = msg.announce_time.strftime_utc('%Y%m%d%H%M%S%f')
        if msg.onset_time and msg.onset_time > _sane_time:
            ots = msg.onset_time.strftime_utc('%Y%m%d%H%M%S%f')
        if msg.end_time and msg.end_time > _sane_time:
            ets = msg.end_time.strftime_utc('%Y%m%d%H%M%S%f')

        lbl = msg.label
        logfile.append('%s,%s,%s,%s,%s,%s' % (sn, ts, ats, ots, ets, lbl))
        source.ack_message(chn, msg, self)

    def flush(self):
        self.archive.flush()
