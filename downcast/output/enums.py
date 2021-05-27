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

from ..messages import EnumerationValueMessage
from .wfdb import (Annotator, AnnotationType)

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

        # Dump original message to BCP file if desired
        if record.dump(msg):
            source.ack_message(chn, msg, self)
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

# Known DWC annotation codes, and corresponding WFDB anntyp / subtyp / aux
_ann_code = {
    b'148631': (AnnotationType.NORMAL,  0, None), # N - normal
    b'148767': (AnnotationType.PVC,     0, None), # V - ventricular
    b'147983': (AnnotationType.SVPB,    0, None), # S - supraventricular
    b'148063': (AnnotationType.PACE,    0, None), # P - paced (most common?)
    b'147543': (AnnotationType.PACE,    1, None), # P - paced
    b'147591': (AnnotationType.PACE,    2, None), # P - paced (least common?)
    b'147631': (AnnotationType.PACESP,  0, None), # ' - single pacer spike
    b'148751': (AnnotationType.PACESP,  1, None), # " - bivent. pacer spike
    b'148783': (AnnotationType.LEARN,   0, None), # L - learning
    b'147551': (AnnotationType.NOTE,    0, b'M'), # M - missed beat
    b'195396': (AnnotationType.UNKNOWN, 0, None), # B - QRS, unspecified type
    b'148759': (AnnotationType.UNKNOWN, 1, None), # ? - QRS, unclassifiable
    b'147527': (AnnotationType.ARFCT,   0, None), # A - artifact
    b'148743': (AnnotationType.NOTE,    0, b'_'), # I - signals inoperable
}

# Unknown annotations are mapped to an anntyp based on the first
# letter of the label
_ann_letter = {
    b'N': AnnotationType.NORMAL,
    b'V': AnnotationType.PVC,
    b'S': AnnotationType.SVPB,
    b'P': AnnotationType.PACE,
    b"'": AnnotationType.PACESP,
    b'"': AnnotationType.PACESP,
    b'L': AnnotationType.LEARN,
    b'M': AnnotationType.NOTE,
    b'B': AnnotationType.UNKNOWN,
    b'?': AnnotationType.UNKNOWN,
    b'A': AnnotationType.ARFCT,
}

class EnumerationValueFinalizer:
    def __init__(self, record):
        self.record = record
        self.log = record.open_log_reader('_phi_enums', allow_missing = True)

        # Scan the enums log file, and add timestamps to the time map.
        for (sn, ts, line) in self.log.unsorted_items():
            ts = datetime.strptime(str(ts), '%Y%m%d%H%M%S%f')
            ts = ts.replace(tzinfo = timezone.utc)
            record.time_map.add_time(ts)

    def finalize_record(self):
        sn0 = self.record.seqnum0()
        if sn0 is None:
            # if we don't have a seqnum0 then time is meaningless
            return

        annfname = os.path.join(self.record.path, 'waves.beat')
        with Annotator(annfname, afreq = 1000) as anns:
            # Reread the enums log file in order, and write beat annotations.
            for (sn, ts, line) in self.log.sorted_items():
                if b'\030' in line:
                    continue
                ts = datetime.strptime(str(ts), '%Y%m%d%H%M%S%f')
                ts = ts.replace(tzinfo = timezone.utc)
                sn = self.record.time_map.get_seqnum(ts, sn + 5120) or sn

                f = line.split(b'\t')
                if len(f) == 3 and f[0] == b'Annot':
                    (label, value_physio_id, value) = f
                    t = _ann_code.get(value_physio_id)
                    if t:
                        (anntyp, subtyp, aux) = t
                    else:
                        anntyp = _ann_letter.get(value[:1],
                                                 AnnotationType.UNKNOWN)
                        subtyp = 0
                        aux = b'[' + value_physio_id + b'] ' + value
                    anns.put(time = (sn - sn0), chan = 255,
                             anntyp = anntyp, subtyp = subtyp, aux = aux)
