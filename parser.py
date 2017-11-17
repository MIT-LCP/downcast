import re
from uuid import UUID
from decimal import Decimal
from datetime import date, datetime, timezone, timedelta

from timestamp import T
from messages import (WaveSampleMessage, AlertMessage,
                      EnumerationValueMessage, NumericValueMessage,
                      BedTagMessage, PatientDateAttributeMessage,
                      PatientStringAttributeMessage,
                      PatientBasicInfoMessage, PatientMappingMessage)
from attributes import (WaveAttr, NumericAttr, EnumerationAttr)

class MessageParser:
    """Abstract class for parsing messages from the database."""
    def __init__(self, dialect = 'ms', paramstyle = 'format'):
        self.dialect = dialect
        self.paramstyle = paramstyle
        if paramstyle == 'qmark':
            self._pmark = '?'
        elif paramstyle == 'format' or paramstyle == 'pyformat':
            self._pmark = '%s'
        else:
            raise ValueError('unknown paramstyle')

    def _gen_query(self, limit, table, columns, constraints, order):
        qstr = 'SELECT '
        if limit is not None and self.dialect == 'ms':
            qstr += ('TOP %d ' % limit)
        qstr += ','.join(columns) + ' FROM ' + table
        params = []
        if len(constraints) > 0:
            qstr += ' WHERE '
            qstr += ' AND '.join(c[0] + self._pmark for c in constraints)
            params = list(c[1] for c in constraints)
        if order is not None:
            qstr += (' ORDER BY ' + order)
        if limit is not None and self.dialect != 'ms':
            qstr += (' LIMIT %d' % limit)
        return (qstr, tuple(params))

class SimpleMessageParser(MessageParser):
    """Abstract class for parsing single-row messages.

    Classes derived from this one must provide a function 'table'
    which returns the name of the table to be queried, a function
    'order' which defines the order (normally a column name), and a
    function 'parse_columns' which constructs the message object based
    on the columns of that table.

    Additional constraints ('where' clauses) can be added to the query
    by calling 'add_constraint' in the constructor.
    """
    def __init__(self, limit, **kwargs):
        MessageParser.__init__(self, **kwargs)
        self.limit = limit
        self._constraints = []

    def add_constraint(self, expr, param):
        self._constraints.append((expr, param))

    def queries(self):
        columns = []
        indices = {}

        # Call parse_columns with a dummy column function (which
        # always returns None), in order to generate the list of
        # columns that we want to query.
        def add_column(name, conv, mandatory = False):
            if name not in indices:
                indices[name] = len(columns)
                columns.append(name)
            return None
        self.parse_columns(None, add_column)

        query = self._gen_query(limit = self.limit,
                                table = self.table(),
                                columns = columns,
                                constraints = self._constraints,
                                order = self.order())

        def handle_row(origin, row):
            def parse_column(name, conv, mandatory = False):
                value = row[indices[name]]
                if value is None and not mandatory:
                    return None
                try:
                    return conv(value)
                except Exception as e:
                    raise TypeError('column %s is %r, not %s'
                                    % (name, value, conv.__name__)) from e
            return self.parse_columns(origin, parse_column)

        return [(query, handle_row)]

class TimestampMessageParser(SimpleMessageParser):
    """Abstract class for parsing record data messages.

    This class can be used for tables that include 'TimeStamp'
    columns.
    """
    def __init__(self, time = None, time_ge = None, time_le = None,
                 time_gt = None, time_lt = None, **kwargs):
        SimpleMessageParser.__init__(self, **kwargs)

        if time is not None:
            self.add_constraint('TimeStamp = ', _to_timestamp(time))
        if time_ge is not None:
            self.add_constraint('TimeStamp >= ', _to_timestamp(time_ge))
        if time_le is not None:
            self.add_constraint('TimeStamp <= ', _to_timestamp(time_le))
        if time_gt is not None:
            self.add_constraint('TimeStamp > ', _to_timestamp(time_gt))
        if time_lt is not None:
            self.add_constraint('TimeStamp < ', _to_timestamp(time_lt))

class MappingIDMessageParser(TimestampMessageParser):
    """Abstract class for parsing record data messages.

    This class can be used for tables that include 'MappingId',
    'TimeStamp', and 'SequenceNumber' columns.
    """
    def __init__(self, mapping_id = None, seqnum = None,
                 seqnum_ge = None, seqnum_le = None,
                 seqnum_gt = None, seqnum_lt = None, **kwargs):
        TimestampMessageParser.__init__(self, **kwargs)

        # XXX Does the order we apply constraints make any difference?
        # Guessing not but it might be worth looking into.

        # Be careful: pymssql allows you to pass a datetime as a '%s'
        # parameter, but the result is wrong.

        if mapping_id is not None:
            self.add_constraint('MappingId = ', _to_uuid(mapping_id))

        if seqnum is not None:
            self.add_constraint('SequenceNumber = ', seqnum)
        if seqnum_ge is not None:
            self.add_constraint('SequenceNumber >= ', seqnum_ge)
        if seqnum_le is not None:
            self.add_constraint('SequenceNumber <= ', seqnum_le)
        if seqnum_gt is not None:
            self.add_constraint('SequenceNumber > ', seqnum_gt)
        if seqnum_lt is not None:
            self.add_constraint('SequenceNumber < ', seqnum_lt)

################################################################

# Accept either a UUID object or a string.
def _uuid(value):
    if isinstance(value, UUID):
        return value
    else:
        return UUID(value)

def _to_uuid(value):
    return _uuid(value)

def _timestamp(value):
    return T(value)

def _to_timestamp(value):
    return str(T(value))

# Accept a string of the form
#  YEAR-MONTH-DAY HOUR:MINUTE:SECOND
date_pattern = re.compile('\A(\d+)-(\d+)-(\d+) \d+:\d+:\d+\Z')
def _date(value):
    m = date_pattern.match(value)
    return date(year = int(m.group(1)),
                month = int(m.group(2)),
                day = int(m.group(3)))

def _integer(value):
    if isinstance(value, int):
        return value
    else:
        raise TypeError()

def _real(value):
    if isinstance(value, Decimal):
        return value
    else:
        raise TypeError()

def _string(value):
    if isinstance(value, str):
        return value
    else:
        raise TypeError()

def _bytes(value):
    if isinstance(value, bytes):
        return value
    else:
        raise TypeError()

def _boolean(value):
    if isinstance(value, bool):
        return value
    else:
        raise TypeError()

################################################################

class WaveSampleParser(MappingIDMessageParser):
    """Parser for wave sample messages."""
    def table(self):
        return '_Export.WaveSample_'
    def order(self):
        return 'TimeStamp'
    def parse_columns(self, origin, cols):
        return WaveSampleMessage(
            origin              = origin,
            wave_id             = cols('WaveId',             _integer, True),
            timestamp           = cols('TimeStamp',          _timestamp, True),
            sequence_number     = cols('SequenceNumber',     _integer, True),
            wave_samples        = cols('WaveSamples',        _bytes, True),
            invalid_samples     = cols('InvalidSamples',     _string),
            unavailable_samples = cols('UnavailableSamples', _string),
            paced_pulses        = cols('PacedPulses',        _string),
            mapping_id          = cols('MappingId',          _uuid, True));

class AlertParser(MappingIDMessageParser):
    """Parser for alert messages."""
    def table(self):
        return '_Export.Alert_'
    def order(self):
        return 'TimeStamp'
    def parse_columns(self, origin, cols):
        return AlertMessage(
            origin          = origin,
            timestamp       = cols('TimeStamp',      _timestamp, True),
            sequence_number = cols('SequenceNumber', _integer, True),
            alert_id        = cols('AlertId',        _uuid),
            source          = cols('Source',         _integer),
            code            = cols('Code',           _integer),
            label           = cols('Label',          _string, True),
            severity        = cols('Severity',       _integer),
            kind            = cols('Kind',           _integer),
            is_silenced     = cols('IsSilenced',     _boolean),
            subtype_id      = cols('SubtypeId',      _integer),
            announce_time   = cols('AnnounceTime',   _timestamp, True),
            onset_time      = cols('OnsetTime',      _timestamp, True),
            end_time        = cols('EndTime',        _timestamp, True),
            mapping_id      = cols('MappingId',      _uuid, True))

class NumericValueParser(MappingIDMessageParser):
    """Parser for numeric value messages."""
    def table(self):
        return '_Export.NumericValue_'
    def order(self):
        return 'TimeStamp'
    def parse_columns(self, origin, cols):
        return NumericValueMessage(
            origin            = origin,
            numeric_id        = cols('NumericId',       _integer, True),
            timestamp         = cols('TimeStamp',       _timestamp, True),
            sequence_number   = cols('SequenceNumber',  _integer, True),
            is_trend_uploaded = cols('IsTrendUploaded', _boolean),
            compound_value_id = cols('CompoundValueId', _uuid),
            value             = cols('Value',           _real),
            mapping_id        = cols('MappingId',       _uuid, True))

class EnumerationValueParser(MappingIDMessageParser):
    """Parser for enumeration value messages."""
    def table(self):
        return '_Export.EnumerationValue_'
    def order(self):
        return 'TimeStamp'
    def parse_columns(self, origin, cols):
        return EnumerationValueMessage(
            origin            = origin,
            enumeration_id    = cols('EnumerationId',   _integer, True),
            timestamp         = cols('TimeStamp',       _timestamp, True),
            sequence_number   = cols('SequenceNumber',  _integer, True),
            compound_value_id = cols('CompoundValueId', _uuid),
            value             = cols('Value',           _string),
            mapping_id        = cols('MappingId',       _uuid, True))


################################################################

class WaveAttrParser(SimpleMessageParser):
    """Parser for wave attributes."""
    def __init__(self, wave_id = None, **kwargs):
        SimpleMessageParser.__init__(self, **kwargs)
        if wave_id is not None:
            self.add_constraint('Id = ', wave_id)

    def table(self):
        return '_Export.Wave_'
    def order(self):
        return None
    def parse_columns(self, origin, cols):
        # sample_period is needed for proper waveform processing.
        # All other attributes are informational only.
        return WaveAttr(
            base_physio_id           = cols('BasePhysioId',        _integer),
            physio_id                = cols('PhysioId',            _integer),
            label                    = cols('Label',               _string),
            channel                  = cols('Channel',             _integer),
            sample_period            = cols('SamplePeriod', _integer, True),
            is_slow_wave             = cols('IsSlowWave',          _boolean),
            is_derived               = cols('IsDerived',           _boolean),
            color                    = cols('Color',               _integer),
            low_edge_frequency       = cols('LowEdgeFrequency',    _real),
            high_edge_frequency      = cols('HighEdgeFrequency',   _real),
            scale_lower              = cols('ScaleLower',          _integer),
            scale_upper              = cols('ScaleUpper',          _integer),
            calibration_scaled_lower = cols('CalibrationScaledLower',
                                            _integer),
            calibration_scaled_upper = cols('CalibrationScaledUpper',
                                            _integer),
            calibration_abs_lower    = cols('CalibrationAbsLower', _real),
            calibration_abs_upper    = cols('CalibrationAbsUpper', _real),
            calibration_type         = cols('CalibrationType',     _integer),
            unit_label               = cols('UnitLabel',           _string),
            unit_code                = cols('UnitCode',            _integer),
            ecg_lead_placement       = cols('EcgLeadPlacement',    _integer))

class NumericAttrParser(SimpleMessageParser):
    """Parser for numeric attributes."""
    def __init__(self, numeric_id = None, **kwargs):
        SimpleMessageParser.__init__(self, **kwargs)
        if numeric_id is not None:
            self.add_constraint('Id = ', numeric_id)

    def table(self):
        return '_Export.Numeric_'
    def order(self):
        return None
    def parse_columns(self, origin, cols):
        return NumericAttr(
            base_physio_id  = cols('BasePhysioId',  _integer),
            physio_id       = cols('PhysioId',      _integer),
            label           = cols('Label',         _string),
            is_aperiodic    = cols('IsAperiodic',   _boolean),
            unit_label      = cols('UnitLabel',     _string),
            validity        = cols('Validity',      _integer),
            lower_limit     = cols('LowerLimit',    _real),
            upper_limit     = cols('UpperLimit',    _real),
            is_alarming_off = cols('IsAlarmingOff', _boolean),
            sub_physio_id   = cols('SubPhysioId',   _integer),
            sub_label       = cols('SubLabel',      _string),
            color           = cols('Color',         _integer),
            is_manual       = cols('IsManual',      _boolean),
            max_values      = cols('MaxValues',     _integer),
            scale           = cols('Scale',         _integer))

class EnumerationAttrParser(SimpleMessageParser):
    """Parser for enumeration attributes."""
    def __init__(self, enumeration_id = None, **kwargs):
        SimpleMessageParser.__init__(self, **kwargs)
        if enumeration_id is not None:
            self.add_constraint('Id = ', enumeration_id)

    def table(self):
        return '_Export.Enumeration_'
    def order(self):
        return None
    def parse_columns(self, origin, cols):
        return EnumerationAttr(
            base_physio_id  = cols('BasePhysioId',  _integer),
            physio_id       = cols('PhysioId',      _integer),
            label           = cols('Label',         _string),
            value_physio_id = cols('ValuePhysioId', _integer),
            is_aperiodic    = cols('IsAperiodic',   _boolean),
            is_manual       = cols('IsManual',      _boolean),
            validity        = cols('Validity',      _integer),
            unit_code       = cols('UnitCode',      _integer),
            unit_label      = cols('UnitLabel',     _string),
            color           = cols('Color',         _integer))

################################################################

class BedTagParser(TimestampMessageParser):
    """Parser for bed tags."""
    def __init__(self, bed_label = None, **kwargs):
        TimestampMessageParser.__init__(self, **kwargs)
        if bed_label is not None:
            self.add_constraint('BedLabel = ', bed_label)

    def table(self):
        return '_Export.BedTag_'
    def order(self):
        return 'Timestamp'
    def parse_columns(self, origin, cols):
        return BedTagMessage(
            origin    = origin,
            bed_label = cols('BedLabel',  _string, True),
            timestamp = cols('Timestamp', _timestamp, True),
            tag       = cols('Tag',       _string, True))

class PatientDateAttributeParser(TimestampMessageParser):
    """Parser for patient date attributes."""
    def __init__(self, patient_id = None, attr = None, **kwargs):
        TimestampMessageParser.__init__(self, **kwargs)
        if patient_id is not None:
            self.add_constraint('PatientId = ', _to_uuid(patient_id))
        if attr is not None:
            self.add_constraint('Name = ', attr)

    def table(self):
        return '_Export.PatientDateAttribute_'
    def order(self):
        return 'Timestamp'
    def parse_columns(self, origin, cols):
        return PatientDateAttributeMessage(
            origin     = origin,
            patient_id = cols('PatientId', _uuid, True),
            timestamp  = cols('Timestamp', _timestamp, True),
            name       = cols('Name',      _string, True),
            value      = cols('Value',     _date))

class PatientStringAttributeParser(TimestampMessageParser):
    """Parser for patient string attributes."""
    def __init__(self, patient_id = None, attr = None, **kwargs):
        TimestampMessageParser.__init__(self, **kwargs)
        if patient_id is not None:
            self.add_constraint('PatientId = ', _to_uuid(patient_id))
        if attr is not None:
            self.add_constraint('Name = ', attr)

    def table(self):
        return '_Export.PatientStringAttribute_'
    def order(self):
        return 'Timestamp'
    def parse_columns(self, origin, cols):
        return PatientStringAttributeMessage(
            origin     = origin,
            patient_id = cols('PatientId', _uuid, True),
            timestamp  = cols('Timestamp', _timestamp, True),
            name       = cols('Name',      _string, True),
            value      = cols('Value',     _string))

class PatientBasicInfoParser(TimestampMessageParser):
    """Parser for patient basic info."""
    def __init__(self, patient_id = None, **kwargs):
        TimestampMessageParser.__init__(self, **kwargs)
        if patient_id is not None:
            self.add_constraint('Id = ', _to_uuid(patient_id))

    def table(self):
        return '_Export.Patient_'
    def order(self):
        return 'Timestamp'
    def parse_columns(self, origin, cols):
        return PatientBasicInfoMessage(
            origin               = origin,
            patient_id           = cols('Id',                _uuid, True),
            timestamp            = cols('Timestamp',         _timestamp, True),
            bed_label            = cols('BedLabel',            _string),
            alias                = cols('Alias',               _string),
            category             = cols('Category',            _integer),
            height               = cols('Height',              _integer),
            height_unit          = cols('HeightUnit',          _integer),
            weight               = cols('Weight',              _integer),
            weight_unit          = cols('WeightUnit',          _integer),
            pressure_unit        = cols('PressureUnit',        _integer),
            paced_mode           = cols('PacedMode',           _integer),
            resuscitation_status = cols('ResuscitationStatus', _integer),
            admit_state          = cols('AdmitState',          _integer),
            clinical_unit        = cols('ClinicalUnit',        _string),
            gender               = cols('Gender',              _integer))

class PatientMappingParser(TimestampMessageParser):
    """Parser for patient mapping info."""
    def __init__(self, patient_id = None, mapping_id = None,
                 hostname = None, **kwargs):
        TimestampMessageParser.__init__(self, **kwargs)
        if mapping_id is not None:
            self.add_constraint('Id = ', _to_uuid(mapping_id))
        if patient_id is not None:
            self.add_constraint('PatientId = ', _to_uuid(patient_id))
        if hostname is not None:
            self.add_constraint('Hostname = ', hostname)

    def table(self):
        return '_Export.PatientMapping_'
    def order(self):
        return 'Timestamp'
    def parse_columns(self, origin, cols):
        return PatientMappingMessage(
            origin            = origin,
            mapping_id        = cols('Id',        _uuid, True),
            patient_id        = cols('PatientId', _uuid, True),
            timestamp         = cols('Timestamp', _timestamp, True),
            is_mapped         = cols('IsMapped',  _boolean),
            hostname          = cols('Hostname',  _string))
