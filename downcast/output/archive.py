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

import datetime
import os
import sys
import re
import json

from ..timestamp import T, delta_ms, very_old_timestamp
from ..util import fdatasync
from ..messages import bcp_format_message
from .files import ArchiveLogFile, ArchiveBinaryFile
from .timemap import TimeMap
from .process import WorkerProcess
from .waveforms import (WaveSampleHandler, WaveSampleFinalizer)
from .enums import (EnumerationValueHandler, EnumerationValueFinalizer)
from .numerics import (NumericValueHandler, NumericValueFinalizer)
from .alerts import (AlertHandler, AlertFinalizer)
from .mapping import PatientMappingHandler
from .patients import PatientHandler
from .log import ArchiveLogReader

def _subdirs(dirname):
    for f in os.listdir(dirname):
        p = os.path.join(dirname, f)
        if os.path.isdir(p):
            yield (p, f)

class Archive:
    def __init__(self, base_dir, deterministic_output = False):
        self.base_dir = base_dir
        self.prefix_length = 2
        self.records = {}
        self.split_interval = 60 * 60 * 1000 # ~ one hour
        self.deterministic_output = deterministic_output
        self.finalization_processes = []

        horizon_file = os.path.join(base_dir, '%horizon')
        try:
            with open(horizon_file) as hf:
                horizon_time = T(hf.read().strip())
        except FileNotFoundError:
            horizon_time = very_old_timestamp
        self.dump_records_before = horizon_time + datetime.timedelta(
            milliseconds = self.split_interval)

        pat = re.compile('\A([A-Za-z0-9-]+)_([0-9a-f-]+)_([-0-9]+)\Z',
                         re.ASCII)

        # Find all existing records in 'base_dir' as well as immediate
        # subdirectories of 'base_dir'
        for (subdir, base) in _subdirs(self.base_dir):
            m = pat.match(base)
            if m is not None:
                self._open_record(path = subdir,
                                  servername = m.group(1),
                                  record_id = m.group(2),
                                  datestamp = m.group(3))
            else:
                for (subdir2, base2) in _subdirs(subdir):
                    m = pat.match(base2)
                    if m is not None:
                        self._open_record(path = subdir2,
                                          servername = m.group(1),
                                          record_id = m.group(2),
                                          datestamp = m.group(3))

    def _open_record(self, path, servername, record_id, datestamp):
        rec = self.records.get((servername, record_id))
        if rec is None or rec.datestamp < datestamp:
            rec = ArchiveRecord(
                path = path,
                servername = servername,
                record_id = record_id,
                datestamp = datestamp)
            # If the record has already been finalized, ignore it
            if not rec.finalized():
                self.records[servername, record_id] = rec
                # If the record had begun to be finalized, restart
                if rec.finalizing():
                    self._finalize_record(rec)

    def _finalize_record(self, rec):
        # Mark the record as finalizing - if the program is
        # interrupted and restarted, we won't write any more data to
        # this record, but restart finalization immediately
        rec.set_finalizing()
        rec.flush(self.deterministic_output)

        # Remove it from the list of active records
        self.records.pop((rec.servername, rec.record_id), None)

        # Start a child process
        proc = WorkerProcess(target = rec.finalize,
                             name = ('finalize-%s' % rec.record_id))
        proc.start()
        self.finalization_processes.append((rec.record_id, proc))

    def get_record(self, message):
        servername = message.origin.servername

        mapping_id = getattr(message, 'mapping_id', None)
        if mapping_id is not None:
            patient_id = message.origin.get_patient_id(mapping_id, True)
            if patient_id is not None:
                record_id = str(patient_id)
            else:
                record_id = str(mapping_id)
        else:
            patient_id = message.patient_id
            record_id = str(patient_id)

        rec = self.records.get((servername, record_id))

        # Check if record needs to be split (interval between
        # consecutive messages exceeds split_interval.)

        # This is done based on timestamps, which is bogus.  It also
        # ignores the inherent skewing between different message
        # types.  But everything about record splitting is slightly
        # bogus and ad-hoc.

        timestamp = message.timestamp

        if rec is not None:
            end = rec.end_time()
            if end is None:
                rec.set_end_time(timestamp)
            else:
                n = delta_ms(timestamp, end)
                if n > self.split_interval:
                    print('%s: splitting between %s and %s'
                          % (record_id, end, timestamp))
                    sys.stdout.flush()
                    self._finalize_record(rec)
                    rec = None
                elif n > 0:
                    rec.set_end_time(timestamp)

        # Create a new record if needed
        if rec is None:
            print('%s: new record at %s' % (record_id, timestamp))
            sys.stdout.flush()
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
            rec.set_end_time(timestamp)
            if timestamp < self.dump_records_before:
                rec.set_dump_mode()

        return rec

    def flush(self):
        for rec in self.records.values():
            rec.flush(self.deterministic_output)
        while self.finalization_processes:
            (record_id, proc) = self.finalization_processes.pop()
            proc.join()
            if proc.exitcode != 0:
                raise Exception('Failed to finalize record %s' % record_id)

    def finalize_before(self, timestamp):
        """Finalize records for which the time limit has expired.

        This indicates that all messages prior to the given timestamp
        have now been processed.  For any record where we have not
        seen any messages within the last hour (split_interval), it is
        now safe to finalize the record.

        This must be called periodically, since otherwise nothing
        would finalize records at the end of the patient's stay.
        """
        recs = list(self.records.values())
        for rec in recs:
            end = rec.end_time()
            if end is not None:
                n = delta_ms(timestamp, end)
                if n > self.split_interval:
                    print('%s: splitting after %s (no more data before %s)'
                          % (rec.record_id, end, timestamp))
                    sys.stdout.flush()
                    self._finalize_record(rec)

    def terminate(self):
        while self.records:
            (_, rec) = self.records.popitem()
            print('%s: terminating at %s' % (rec.record_id, rec.end_time()))
            sys.stdout.flush()
            self._finalize_record(rec)

class ArchiveRecord:
    def __init__(self, path, servername, record_id, datestamp, create = False):
        self.path = path
        self.servername = servername
        self.record_id = record_id
        self.datestamp = datestamp
        self.files = {}
        if create:
            os.makedirs(self.path, exist_ok = True)

        self.properties = self._read_state_file('_phi_properties')
        self.time_map = TimeMap(record_id)
        self.time_map.read(path, '_phi_time_map')
        self._base_seqnum = self.get_int_property(['base_sequence_number'])
        self._end_time = self.get_timestamp_property(['end_time'])
        self.modified = False
        self.dump_messages = self.get_int_property(['dump'])

    def seqnum0(self):
        return self._base_seqnum

    def set_seqnum0(self, seqnum):
        self._base_seqnum = seqnum
        self.modified = True

    def end_time(self):
        return self._end_time

    def set_end_time(self, time):
        self._end_time = time
        self.modified = True

    def set_dump_mode(self):
        self.set_property(['dump'], 1)
        self.dump_messages = 1

    def finalized(self):
        return (self.get_int_property(['finalized']) == 1)

    def finalizing(self):
        return (self.get_int_property(['finalized']) == 0)

    def set_finalizing(self):
        for f in self.files.values():
            f.close()
        self.files = {}
        self.set_property(['finalized'], 0)

    def finalize(self):
        wsf = WaveSampleFinalizer(self)
        nvf = NumericValueFinalizer(self)
        evf = EnumerationValueFinalizer(self)
        alf = AlertFinalizer(self)

        self.time_map.resolve_gaps()

        wsf.finalize_record()
        nvf.finalize_record()
        evf.finalize_record()
        alf.finalize_record()

        for f in self.files.values():
            f.close()
        self.files = {}
        self.set_property(['finalized'], 1)
        self.flush(True)

    def flush(self, deterministic = False):
        for f in self.files.values():
            f.flush()
        if self.modified:
            self.set_property(['base_sequence_number'], self._base_seqnum)
            self.set_property(['end_time'], str(self._end_time))
            self.time_map.write(self.path, '_phi_time_map')
            self._write_state_file('_phi_properties', self.properties,
                                   deterministic = deterministic)
            self.dir_sync()

    def dir_sync(self):
        d = os.open(self.path, os.O_RDONLY|os.O_DIRECTORY)
        try:
            fdatasync(d)
        finally:
            os.close(d)

    def dump(self, message):
        if not self.dump_messages:
            return False

        datafilename = '_phi_' + type(message).__name__
        logfile = self.open_log_file(datafilename)
        logfile.append_raw(bcp_format_message(message))
        return True

    def _read_state_file(self, name):
        fname = os.path.join(self.path, name)
        try:
            with open(fname, 'rt', encoding = 'UTF-8') as f:
                return json.load(f)
        except (FileNotFoundError, UnicodeError, ValueError):
            return None

    def _write_state_file(self, name, content, deterministic = False):
        fname = os.path.join(self.path, name)
        tmpfname = os.path.join(self.path, '_' + name + '.tmp')
        with open(tmpfname, 'wt', encoding = 'UTF-8') as f:
            json.dump(content, f, sort_keys = deterministic)
            f.write('\n')
            f.flush()
            fdatasync(f.fileno())
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

    def set_time(self, seqnum, time):
        self.time_map.set_time(seqnum, time)
        self.modified = True

    def get_int_property(self, path, default = None):
        try:
            return int(self.get_property(path))
        except (KeyError, TypeError):
            return default

    def get_str_property(self, path, default = None):
        try:
            value = self.get_property(path)
            if isinstance(value, str):
                return value
            else:
                return default
        except (KeyError, TypeError):
            return default

    def get_timestamp_property(self, path, default = None):
        try:
            return T(str(self.get_property(path)))
        except (KeyError, TypeError, ValueError):
            return default

    def open_log_file(self, name, truncate = False):
        if name not in self.files:
            fname = os.path.join(self.path, name)
            if truncate:
                try:
                    os.unlink(fname)
                except FileNotFoundError:
                    pass
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

    def open_log_reader(self, name, allow_missing = False):
        fname = os.path.join(self.path, name)
        return ArchiveLogReader(fname, allow_missing = allow_missing)
