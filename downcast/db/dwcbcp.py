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

import os
import re

from .bcp import *

# Sorting order for each table

_table_order_column = {
    '_Export.Alert_':                  'TimeStamp',
    '_Export.BedTag_':                 'Timestamp',
    '_Export.Enumeration_':            'Id',
    '_Export.EnumerationValue_':       'TimeStamp',
    '_Export.Numeric_':                'Id',
    '_Export.NumericValue_':           'TimeStamp',
    '_Export.Patient_':                'Timestamp',
    '_Export.PatientDateAttribute_':   'Timestamp',
    '_Export.PatientStringAttribute_': 'Timestamp',
    '_Export.PatientMapping_':         'Timestamp',
    '_Export.Wave_':                   'Id',
    '_Export.WaveSample_':             'TimeStamp'
}

# Index keys for each table

_table_id_columns = {
    '_Export.PatientMapping_': ['Id']
}

# Regular expression to identify start of a row

_table_sync_pattern = {
    '_Export.Alert_':                  b'\n().',
    '_Export.BedTag_':                 b'\n().',
    '_Export.Enumeration_':            b'\n().',
    '_Export.EnumerationValue_':       b'\n().',
    '_Export.Numeric_':                b'\n().',
    '_Export.NumericValue_':           b'\n().',
    '_Export.Patient_':                b'\n().',
    '_Export.PatientDateAttribute_':   b'\n().',
    '_Export.PatientStringAttribute_': b'\n().',
    '_Export.PatientMapping_':         b'\n().',
    '_Export.Wave_':                   b'\n().',
    '_Export.WaveSample_': b'''(?x)
        # UnavailableSamples
        [ 0-9\0]* [\t]
        # InvalidSamples
        [ 0-9\0]* [\t]
        # PacedPulses
        [ 0-9\0]* [\t]
        # MappingId
        [0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12} [\n]
        ()
        # WaveId
        \d+ [\t]
        # TimeStamp
        \d{4}-\d{2}-\d{2} [ ] \d{2}:\d{2}:\d{2}\.\d+ [ ] [-+]\d{2}:\d{2} [\t]
        # SequenceNumber
        \d+ [\t]
    '''
}

# List of columns and types

_table_columns = {
    '_Export.Alert_': {
        'TimeStamp':               DATETIME,
        'SequenceNumber':          INTEGER,
        'AlertId':                 UUID,
        'Source':                  INTEGER,
        'Code':                    INTEGER,
        'Label':                   STRING,
        'Severity':                INTEGER,
        'Kind':                    INTEGER,
        'IsSilenced':              BOOLEAN,
        'SubtypeId':               INTEGER,
        'AnnounceTime':            DATETIME,
        'OnsetTime':               DATETIME,
        'EndTime':                 DATETIME,
        'MappingId':               UUID,
    },
    '_Export.BedTag_': {
        'BedLabel':                STRING,
        'Timestamp':               DATETIME,
        'Tag':                     STRING,
    },
    '_Export.Enumeration_': {
        'Id':                      INTEGER,
        'BasePhysioId':            INTEGER,
        'PhysioId':                INTEGER,
        'Label':                   STRING,
        'ValuePhysioId':           INTEGER,
        'IsAperiodic':             BOOLEAN,
        'IsManual':                BOOLEAN,
        'Validity':                INTEGER,
        'UnitCode':                INTEGER,
        'UnitLabel':               STRING,
        'Color':                   INTEGER,
    },
    '_Export.EnumerationValue_': {
        'EnumerationId':           INTEGER,
        'TimeStamp':               DATETIME,
        'SequenceNumber':          INTEGER,
        'CompoundValueId':         UUID,
        'Value':                   STRING,
        'MappingId':               UUID,
    },
    '_Export.Numeric_': {
        'Id':                      INTEGER,
        'BasePhysioId':            INTEGER,
        'PhysioId':                INTEGER,
        'Label':                   STRING,
        'IsAperiodic':             BOOLEAN,
        'UnitLabel':               STRING,
        'Validity':                INTEGER,
        'LowerLimit':              NUMBER,
        'UpperLimit':              NUMBER,
        'IsAlarmingOff':           BOOLEAN,
        'SubPhysioId':             INTEGER,
        'SubLabel':                STRING,
        'Color':                   INTEGER,
        'IsManual':                BOOLEAN,
        'MaxValues':               INTEGER,
        'Scale':                   INTEGER,
    },
    '_Export.NumericValue_': {
        'NumericId':               INTEGER,
        'TimeStamp':               DATETIME,
        'SequenceNumber':          INTEGER,
        'IsTrendUploaded':         BOOLEAN,
        'CompoundValueId':         UUID,
        'Value':                   NUMBER,
        'MappingId':               UUID,
    },
    '_Export.Patient_': {
        'Id':                      UUID,
        'Timestamp':               DATETIME,
        'BedLabel':                STRING,
        'Alias':                   STRING,
        'Category':                INTEGER,
        'Height':                  NUMBER,
        'HeightUnit':              INTEGER,
        'Weight':                  NUMBER,
        'WeightUnit':              INTEGER,
        'PressureUnit':            INTEGER,
        'PacedMode':               INTEGER,
        'ResuscitationStatus':     INTEGER,
        'AdmitState':              INTEGER,
        'ClinicalUnit':            STRING,
        'Gender':                  INTEGER,
    },
    '_Export.PatientDateAttribute_': {
        'PatientId':               UUID,
        'Timestamp':               DATETIME,
        'Name':                    STRING,
        'Value':                   STRING, # actually a date but who cares
    },
    '_Export.PatientStringAttribute_': {
        'PatientId':               UUID,
        'Timestamp':               DATETIME,
        'Name':                    STRING,
        'Value':                   STRING,
    },
    '_Export.PatientMapping_': {
        'Id':                      UUID,
        'PatientId':               UUID,
        'Timestamp':               DATETIME,
        'IsMapped':                BOOLEAN,
        'Hostname':                STRING,
    },
    '_Export.Wave_': {
        'Id':                      INTEGER,
        'BasePhysioId':            INTEGER,
        'PhysioId':                INTEGER,
        'Label':                   STRING,
        'Channel':                 INTEGER,
        'SamplePeriod':            INTEGER,
        'IsSlowWave':              BOOLEAN,
        'IsDerived':               BOOLEAN,
        'Color':                   INTEGER,
        'LowEdgeFrequency':        NUMBER,
        'HighEdgeFrequency':       NUMBER,
        'ScaleLower':              INTEGER,
        'ScaleUpper':              INTEGER,
        'CalibrationScaledLower':  INTEGER,
        'CalibrationScaledUpper':  INTEGER,
        'CalibrationAbsLower':     NUMBER,
        'CalibrationAbsUpper':     NUMBER,
        'CalibrationType':         INTEGER,
        'UnitLabel':               STRING,
        'UnitCode':                INTEGER,
        'EcgLeadPlacement':        INTEGER,
    },
    '_Export.WaveSample_': {
        'WaveId':                  INTEGER,
        'TimeStamp':               DATETIME,
        'SequenceNumber':          INTEGER,
        'WaveSamples':             BINARY,
        'UnavailableSamples':      STRING,
        'InvalidSamples':          STRING,
        'PacedPulses':             STRING,
        'MappingId':               UUID,
    }
}

class DWCBCPConnection(BCPConnection):
    def __init__(self, datadirs):
        BCPConnection.__init__(self)
        for d in datadirs:
            self.add_data_dir(d)

    def add_data_dir(self, dirname):
        """
        Import a directory of data files into the database.

        An example data directory might contain the following:

            Alert.20010101_20010102
            Alert.fmt
            BedTag.20010101_20010102
            BedTag.fmt
            Enumeration
            Enumeration.fmt
            EnumerationValue.20010101_20010102
            EnumerationValue.fmt
            Numeric
            Numeric.fmt
            NumericValue.20010101_20010102
            NumericValue.fmt
            Patient.20010101_20010102
            Patient.fmt
            PatientDateAttribute.20010101_20010102
            PatientDateAttribute.fmt
            PatientMapping.20010101_20010102
            PatientMapping.fmt
            PatientStringAttribute.20010101_20010102
            PatientStringAttribute.fmt
            Wave
            Wave.fmt
            WaveSample.20010101_20010102
            WaveSample.fmt

        For example, 'Alert.20010101_20010102' contains Alert data
        between those two dates, and 'Alert.fmt' is a freebcp format
        file describing the format of 'Alert.20010101_20010102'.

        The 'Enumeration', 'Numeric', and 'Wave' tables are not
        specific to the time period.  For those tables, the most
        recently imported file replaces any previous files.

        For the other tables, all data files are concatenated in the
        order that they are imported.  All of these files must be
        sorted by timestamp, and must not overlap.
        """

        meta_pat = re.compile('\A(?:Enumeration|Numeric|Wave)(?:\.dat)?\Z')
        data_pat = re.compile('\.(?:dat|[0-9]+_[0-9]+)\Z')
        for f in sorted(os.listdir(dirname)):
            path = os.path.join(dirname, f)
            base = f.split('.')[0]
            table = '_Export.%s_' % base
            fmtpath = os.path.join(dirname, base + '.fmt')
            if meta_pat.search(f):
                self.add_data_file(table, path, fmtpath, True)
            elif data_pat.search(f):
                self.add_data_file(table, path, fmtpath, False)

    def add_data_file(self, table, data_file, format_file, replace = False):
        """
        Import a file into the database.

        table is the name of the table, such as '_Export.Alert_'.

        data_file is the name of the raw data file; format_file is the
        name of the corresponding freebcp format file.  (Note that
        only a very small subset of the possible freebcp formats are
        supported.)

        If replace is true, the new data file replaces all previously
        imported data; otherwise, it is concatenated onto the end of
        the preceding files.
        """

        tbl = self.add_table(table)
        tbl.set_sync_pattern(_table_sync_pattern[table])
        tbl.set_order(_table_order_column[table])
        for (col, dtype) in _table_columns[table].items():
            tbl.add_column(col, dtype)
        for col in _table_id_columns.get(table, []):
            tbl.add_unique_id(col)
        if replace:
            tbl.clear()
        tbl.add_data_file(data_file, format_file)

#### DB-API ####

def connect(datadirs):
    return DWCBCPConnection(datadirs)
