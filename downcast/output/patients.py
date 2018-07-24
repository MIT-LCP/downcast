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

from ..messages import (PatientBasicInfoMessage,
                        PatientDateAttributeMessage,
                        PatientStringAttributeMessage)

class PatientHandler:
    def __init__(self, archive):
        self.archive = archive

    def send_message(self, chn, msg, source, ttl):
        if isinstance(msg, PatientBasicInfoMessage):
            source.nack_message(chn, msg, self)
            record = self.archive.get_record(msg)
            if record is None:
                return
            self._log_info(record, msg, 'BedLabel', msg.bed_label)
            self._log_info(record, msg, 'Alias', msg.alias)
            self._log_info(record, msg, 'Category', msg.category)
            self._log_info(record, msg, 'Height', msg.height)
            self._log_info(record, msg, 'HeightUnit', msg.height_unit)
            self._log_info(record, msg, 'Weight', msg.weight)
            self._log_info(record, msg, 'WeightUnit', msg.weight_unit)
            self._log_info(record, msg, 'PressureUnit', msg.pressure_unit)
            self._log_info(record, msg, 'PacedMode', msg.paced_mode)
            self._log_info(record, msg, 'ResuscitationStatus',
                           msg.resuscitation_status)
            self._log_info(record, msg, 'AdmitState', msg.admit_state)
            self._log_info(record, msg, 'ClinicalUnit', msg.clinical_unit)
            self._log_info(record, msg, 'Gender', msg.gender)
            source.ack_message(chn, msg, self)

        elif isinstance(msg, PatientDateAttributeMessage):
            source.nack_message(chn, msg, self)
            record = self.archive.get_record(msg)
            if record is None:
                return
            self._log_info(record, msg, 'd:%s' % msg.name, msg.value)
            source.ack_message(chn, msg, self)
        elif isinstance(msg, PatientStringAttributeMessage):
            source.nack_message(chn, msg, self)
            record = self.archive.get_record(msg)
            if record is None:
                return
            self._log_info(record, msg, 's:%s' % msg.name, msg.value)
            source.ack_message(chn, msg, self)

    def _log_info(self, record, msg, key, value):
        logfile = record.open_log_file('_phi_patient_info')
        logfile.append('%s,%s,%s' % (msg.timestamp, _escape(key),
                                     _escape(str(value))))

    def flush(self):
        self.archive.flush()

    def finalize_record(record):
        pass

_escape_chars = list(range(32)) + [127] + [ord(x) for x in ',"\'\\']
_escape_table = str.maketrans({x: '\\%03o' % x for x in _escape_chars})
def _escape(s):
    return s.translate(_escape_table)
