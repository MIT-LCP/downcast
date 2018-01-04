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

import os
import re
import json

from ..messages import WaveSampleMessage, NumericValueMessage
from .files import ArchiveLogFile, ArchiveBinaryFile

class Archive:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.prefix_length = 2
        self.records = {}
        self.split_interval = 60 * 60 * 1000 # ~ one hour

    def open_records(self):
        pat = re.compile('\A([A-Za-z0-9-]+)_([0-9a-f-]+)_([-0-9]+)\Z',
                         re.ASCII)

        # Find all existing records in 'base_dir' as well as immediate
        # subdirectories of 'base_dir'
        for f in os.scandir(base_dir):
            if f.is_dir():
                subdir = os.path.join(base_dir, f.name)
                m = pat.match(f.name)
                if m is not None:
                    _open_record(path = subdir,
                                 servername = m.group(1),
                                 record_id = m.group(2),
                                 datestamp = m.group(3))
                else:
                    for g in os.scandir(subdir):
                        m = pat.match(g.name)
                        if m is not None and g.is_dir():
                            _open_record(path = os.path.join(subdir, g.name),
                                         servername = m.group(1),
                                         record_id = m.group(2),
                                         datestamp = m.group(3))

    def _open_record(self, path, servername, record_id, datestamp):
        rec = self.records.get((servername, record_id))
        if rec is None or rec.datestamp < datestamp:
            self.records[servername, record_id] = ArchiveRecord(
                path = path,
                servername = servername,
                record_id = record_id,
                datestamp = datestamp)

    def get_record(self, message, sync):
        servername = message.origin.servername

        mapping_id = getattr(message, 'mapping_id', None)
        if mapping_id is not None:
            patient_id = message.origin.get_patient_id(mapping_id, sync)
            if patient_id is not None:
                record_id = str(patient_id)
            elif sync:
                record_id = str(mapping_id)
            else:
                return None
        else:
            patient_id = message.patient_id
            record_id = str(patient_id)

        rec = self.records.get((servername, record_id))

        # Check if record needs to be split (interval between
        # consecutive WaveSampleMessages or consecutive
        # NumericValueMessages exceeds split_interval.)

        # Note that we are assuming that each message type is
        # processed in roughly-chronological order, and that different
        # message types never get too far out of sync with each other.

        # FIXME: This logic needs improvement.  In particular it won't
        # handle the *end* of a patient stay, and it also won't work
        # if alerts/enums are queried ahead of waves/numerics.
        # Assuming we can't trust TimeStamps, need to use
        # concurrently-processed records to determine when
        # 'split_interval' ticks have elapsed.

        if rec is not None and (isinstance(message, WaveSampleMessage)
                                or isinstance(message, NumericValueMessage)):
            seqnum = message.sequence_number
            if isinstance(message, WaveSampleMessage):
                end = rec.get_int_property(['waves_end'])
            else:
                end = rec.get_int_property(['numerics_end'])
            if end is not None and seqnum - end > self.split_interval:
                self.records[servername, record_id] = None
                rec.finalize()
                rec = None

        # Create a new record if needed
        if rec is None:
            datestamp = message.timestamp.strftime_utc('%Y%m%d-%H%M')
            prefix = record_id[0:self.prefix_length]
            name = '%s_%s_%s' % (servername, record_id, datestamp)
            path = os.path.join(self.base_dir, prefix, name)
            rec = ArchiveRecord(path = path,
                                servername = servername,
                                record_id = record_id,
                                datestamp = datestamp,
                                create = True)
            self.records[servername, record_id] = rec

        # Update time mapping
        rec.add_event(message)

        return rec

    def flush(self):
        for rec in self.records.values():
            rec.flush()

    def terminate(self):
        for rec in self.records.values():
            rec.finalize()
        self.records = {}

class ArchiveRecord:
    def __init__(self, path, servername, record_id, datestamp, create = False):
        self.path = path
        self.servername = servername
        self.record_id = record_id
        self.datestamp = datestamp
        self.files = {}
        if create:
            os.makedirs(self.path, exist_ok = True)

        self.properties = self._read_state_file('_properties')
        self.modified = False

    def add_event(self, message):
        # FIXME: periodically record time stamps in a log file
        st = self.get_int_property(['start_time'])
        t = getattr(message, 'sequence_number', None)
        if st is None and t is not None:
            self.set_property(['start_time'], t)
        if isinstance(message, WaveSampleMessage):
            self.set_property(['waves_end'], t)
        if isinstance(message, NumericValueMessage):
            self.set_property(['numerics_end'], t)

    def seqnum0(self):
        return self.get_int_property(['start_time'])

    def finalize(self):
        # FIXME: lots of stuff to do here...
        for f in self.files:
            f.close()
        self.modified = True
        self.flush()
        return

    def flush(self):
        if self.modified:
            self._write_state_file('_properties', self.properties)
            self.dir_sync()

    def dir_sync(self):
        d = os.open(self.path, os.O_RDONLY|os.O_DIRECTORY)
        try:
            os.fdatasync(d)
        finally:
            os.close(d)

    def _read_state_file(self, name):
        fname = os.path.join(self.path, name)
        try:
            with open(fname, 'rt', encoding = 'UTF-8') as f:
                return json.load(f)
        except (FileNotFoundError, UnicodeError, ValueError):
            return None

    def _write_state_file(self, name, content):
        fname = os.path.join(self.path, name)
        tmpfname = os.path.join(self.path, '_' + name + '.tmp')
        with open(tmpfname, 'wt', encoding = 'UTF-8') as f:
            json.dump(content, f)
            f.flush()
            os.fdatasync(f.fileno())
        os.rename(tmpfname, fname)

    def get_property(self, path):
        v = self.properties
        for k in path:
            v = v[k]
        return v

    def set_property(self, path, value):
        if not isinstance(self.properties, dict):
            self.properties = {}
        v = self.properties
        for k in path[:-1]:
            if k not in v or not isinstance(v[k], dict):
                v[k] = {}
            v = v[k]
        v[path[-1]] = value
        self.modified = True

    def get_int_property(self, path, default = None):
        try:
            return int(self.get_property(path))
        except (KeyError, TypeError):
            return default

    def get_str_property(self, path, default = None):
        try:
            return str(self.get_property(path))
        except (KeyError, TypeError):
            return default

    def open_log_file(self, name):
        if name not in self.files:
            fname = os.path.join(self.path, name)
            self.files[name] = ArchiveLogFile(fname)
            self.modified = True
        return self.files[name]

    def open_bin_file(self, name, **kwargs):
        if name not in self.files:
            fname = os.path.join(self.path, name)
            self.files[name] = ArchiveBinaryFile(fname, **kwargs)
            self.modified = True
        return self.files[name]

    def close_file(self, name):
        if name in self.files:
            self.files[name].close()
            del self.files[name]
