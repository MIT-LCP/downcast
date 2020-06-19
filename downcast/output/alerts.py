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
import os
import re

from ..messages import AlertMessage
from ..timestamp import (T, delta_ms)
from ..util import string_to_ascii
from .wfdb import (Annotator, AnnotationType)

_sane_time = T('1970-01-01 00:00:00.000 +00:00')

class AlertHandler:
    def __init__(self, archive):
        self.archive = archive

    def send_message(self, chn, msg, source, ttl):
        if not isinstance(msg, AlertMessage):
            return

        source.nack_message(chn, msg, self)

        # Look up the corresponding record
        record = self.archive.get_record(msg)
        if record is None:
            # Record not yet available - hold message in pending and
            # continue processing
            return

        # Open or create a log file
        logfile = record.open_log_file('_phi_alerts')

        # Write value to the log file
        sn = msg.sequence_number
        ts = msg.timestamp.strftime_utc('%Y%m%d%H%M%S%f')
        idstr = str(msg.alert_id)
        lbl = string_to_ascii(msg.label)
        if msg.is_silenced:
            statestr = '~'
        else:
            statestr = '='

        logfile.append('S%s' % sn)
        if msg.announce_time and msg.announce_time > _sane_time:
            ats = msg.announce_time.strftime_utc('%Y%m%d%H%M%S%f')
            logfile.append(ats)
            logfile.append('(%s)+' % (idstr,))
        if msg.onset_time and msg.onset_time > _sane_time:
            ots = msg.onset_time.strftime_utc('%Y%m%d%H%M%S%f')
            logfile.append(ots)
            logfile.append('(%s)!' % (idstr,))
        if msg.end_time and msg.end_time > _sane_time:
            ets = msg.end_time.strftime_utc('%Y%m%d%H%M%S%f')
            logfile.append(ets)
            logfile.append('(%s)-' % (idstr,))
        logfile.append(ts)
        logfile.append('(%s)%s%s%s' % (idstr, msg.severity, statestr, lbl))

        source.ack_message(chn, msg, self)

    def flush(self):
        self.archive.flush()

class AlertFinalizer:
    def __init__(self, record):
        self.record = record
        self.log = record.open_log_reader('_phi_alerts', allow_missing = True)

        self.alert_onset = {}
        self.alert_announce = {}
        self.alert_end = {}

        # Scan the alerts log file, add timestamps to the time map,
        # and record onset/announce/end time for each alert ID.
        for (sn, ts, line) in self.log.unsorted_items():
            ts = datetime.strptime(str(ts), '%Y%m%d%H%M%S%f')
            ts = ts.replace(tzinfo = timezone.utc)
            record.time_map.add_time(ts)

            (alert_id, event, severity, state, label) = _parse_info(line)
            # If there are multiple recorded onset times, save the one
            # that was recorded first (smallest sequence number.)
            # Save the earliest onset timestamp that was recorded at
            # that sequence number.
            if event == b'!':
                if (sn, ts) < self.alert_onset.setdefault(alert_id, (sn, ts)):
                    self.alert_onset[alert_id] = (sn, ts)
            # If there are multiple recorded announce times, save the
            # one that was recorded first (smallest sequence number.)
            # Save the earliest announce timestamp that was recorded
            # at that sequence number.
            elif event == b'+':
                if (sn, ts) < self.alert_announce.setdefault(alert_id,
                                                             (sn, ts)):
                    self.alert_announce[alert_id] = (sn, ts)
            # If there are multiple recorded end times, save the one
            # that was recorded last (largest sequence number.)  Save
            # the latest end timestamp that was recorded at that
            # sequence number.
            elif event == b'-':
                if (sn, ts) > self.alert_end.setdefault(alert_id, (sn, ts)):
                    self.alert_end[alert_id] = (sn, ts)

    def finalize_record(self):
        sn0 = self.record.seqnum0()
        if sn0 is None:
            # if we don't have a seqnum0 then time is meaningless
            return

        alert_first = {}
        alert_last = {}
        alert_num = {}

        announce_t = {}
        for (alert_id, (sn, ts)) in self.alert_announce.items():
            sn = self.record.time_map.get_seqnum(ts, sn + 5120)
            if sn is None:
                continue
            announce_t[alert_id] = sn - sn0

        end_t = {}
        for (alert_id, (sn, ts)) in self.alert_end.items():
            # alert end time may actually be slightly later than
            # time of the message.  why?  no idea.  how do these
            # timestamps work in regard to system clock
            # adjustments?  no idea.
            sn = self.record.time_map.get_seqnum(ts, sn + 15120)
            if sn is None:
                continue
            end_t[alert_id] = sn - sn0

        annfname = os.path.join(self.record.path, 'waves.alarm')
        with Annotator(annfname, afreq = 1000) as anns:
            # Reread the alerts log file in order.  Assign an integer
            # ID to each alert in order of appearance, and record the
            # severity, state (silenced or not) and label.  If the
            # severity/state/label changes between the announce time
            # and end time, then add an annotation each time it
            # changes.  The earliest severity/state/label will be
            # applied to the announce and onset annotations, and the
            # latest severity/state/label will be applied to the end
            # annotation.
            for (sn, ts, line) in self.log.sorted_items():
                if b'\030' in line:
                    continue
                ts = datetime.strptime(str(ts), '%Y%m%d%H%M%S%f')
                ts = ts.replace(tzinfo = timezone.utc)
                sn = self.record.time_map.get_seqnum(ts, sn + 5120) or sn
                t = sn - sn0

                (alert_id, event, severity, state, label) = _parse_info(line)
                if alert_id and label:
                    num = alert_num.setdefault(alert_id, len(alert_num) + 1)
                    oldstate = alert_last.get(alert_id, None)
                    newstate = (severity, state, label)
                    alert_first.setdefault(alert_id, newstate)
                    alert_last[alert_id] = newstate
                    announce = announce_t.get(alert_id, t)
                    end = end_t.get(alert_id, t)
                    if (oldstate and oldstate != newstate
                            and announce <= t <= end):
                        _put_annot(anns, t, num, b';', severity, state, label)

            for (alert_id, (sn, ts)) in self.alert_onset.items():
                num = alert_num.get(alert_id)
                sn = self.record.time_map.get_seqnum(ts, sn + 5120)
                if num is None or sn is None:
                    continue
                t = sn - sn0
                (severity, state, label) = alert_first[alert_id]
                _put_annot(anns, t, num, b'+', severity, state, label)

            for (alert_id, t) in announce_t.items():
                num = alert_num.get(alert_id)
                if num is None:
                    continue
                (severity, state, label) = alert_first[alert_id]
                _put_annot(anns, t, num, b'<', severity, state, label)

            for (alert_id, t) in end_t.items():
                num = alert_num.get(alert_id)
                if num is None:
                    continue
                (severity, state, label) = alert_last[alert_id]
                _put_annot(anns, t, num, b'>', severity, state, label)

_info_pattern = re.compile(rb'\(([\w-]+)\)(?:([-+!])|(\d+)([=~])(.*))')

def _parse_info(line):
    m = _info_pattern.fullmatch(line.rstrip(b'\n'))
    if m:
        return m.groups()
    else:
        return (None, None, None, None, None)

def _put_annot(anns, time, alert_num, event_code, severity, state, label):
    severity = int(severity)
    if severity == 0:           # RED
        subtyp = 3
    elif severity == 1:         # YELLOW
        subtyp = 2
    elif severity == 2:         # SHORT YELLOW
        subtyp = 1
    else:
        subtyp = 0

    if event_code == b'+':      # onset
        subtyp += 90
    elif event_code == b'<':    # announce
        subtyp += 80
    elif event_code == b'>':    # end
        subtyp += 60
    else:
        subtyp += 70

    aux = event_code + b'{' + str(alert_num).encode() + b'}'
    if state == b'~':           # silenced
        aux += b'~'
    else:
        aux += b' '
    aux += label

    anns.put(time = time, anntyp = AnnotationType.NOTE,
             subtyp = subtyp, chan = 255, aux = aux)
