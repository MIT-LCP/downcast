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

from ..messages import PatientMappingMessage

class PatientMappingHandler:
    def __init__(self, archive):
        self.archive = archive

    def send_message(self, chn, msg, source, ttl):
        if not isinstance(msg, PatientMappingMessage):
            return

        source.nack_message(chn, msg, self)
        msg.origin.set_patient_id(msg.mapping_id, msg.patient_id)
        source.ack_message(chn, msg, self)

    def flush(self):
        pass

    def finalize_record(record):
        pass
