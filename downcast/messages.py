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

from collections import namedtuple
import struct
import uuid

################################################################

# _Export.WaveSample_
WaveSampleMessage = namedtuple('WaveSampleMessage', (
    # The original data source (required for looking up wave_ids.)
    'origin',

    # An opaque identifier (probably a small integer) for the waveform
    # attributes.  I am hoping that those attributes are immutable
    # (e.g. same signal with different gain/baseline will use a
    # different ID.)  Underlying type is 'bigint'.
    'wave_id',

    # A timestamp (probably from DWC or SQL.)
    'timestamp',

    # Apparently a uniform counter (i.e., runs continuously, never
    # adjusted forward or backward) of Philips milliseconds.
    'sequence_number',

    # Byte array encoding wave samples as 16-bit little-endian
    # unsigned integers.  Note that users should probably assume that
    # indices corresponding to 'unavailable_samples' or
    # 'invalid_samples' contain garbage and should be ignored.
    'wave_samples',

    # String describing the intervals within 'wave_samples' that are
    # considered "unavailable".  Should be a list of ASCII decimal
    # numbers separated by spaces; each pair of numbers indicates the
    # start and end of an "unavailable" interval.
    'unavailable_samples',

    # String describing the intervals within 'wave_samples' that are
    # considered "invalid".  Should be a list of ASCII decimal numbers
    # separated by spaces; each pair of numbers indicates the start
    # and end of an "invalid" interval.  Indices start at zero and the
    # range is inclusive (e.g. "0 9" would indicate the first ten
    # samples.)
    'invalid_samples',

    # String (list of ASCII decimal numbers separated by spaces)
    # giving the relative sample numbers at which pacemaker pulses
    # occurred.
    'paced_pulses',

    # Should correspond to 'mapping_id' in PatientMappingMessage.
    'mapping_id'))

################################################################)

# _Export.Alert_
AlertMessage = namedtuple('AlertMessage', (
    # The original data source.
    'origin',

    # A timestamp (probably from DWC or SQL.)
    'timestamp',

    # Sequence number.  Corresponds to what?
    'sequence_number',

    # An opaque identifier (probably a GUID) for the particular alarm.
    'alert_id',

    # Magic number for the "source" of the alarm.  See
    # System_Parameter-Alerts_Table_Ed_2_-_PIIC_iX_Rel_B.00.xlsx
    # (Parameters? or Calculations?).  Underlying type is 'bigint'.
    'source',

    # Magic number for the "code" of the alarm.  See
    # System_Parameter-Alerts_Table_Ed_2_-_PIIC_iX_Rel_B.00.xlsx
    # (Alarm-Code-Ids).  Underlying type is 'integer'.
    'code',

    # Alarm message.
    'label',

    # Magic number for the "severity" of the alarm.
    'severity',

    # Magic number for the category of the alarm.
    'kind',

    # Indicates that alarm has been silenced (?)
    'is_silenced',

    # Undocumented magic number.  Underlying type is 'bigint'.
    'subtype_id',

    # Time that the alarm is reported? (probably from monitor)
    'announce_time',

    # Time that the triggering condition begins? (probably from monitor)
    # If the time is unknown, this will be something absurd like 0001-01-01.
    'onset_time',

    # Time that ??? ends (probably from monitor)
    # If the alarm has not yet ended, this will be something absurd
    # like 0001-01-01.
    'end_time',

    # Should correspond to 'mapping_id' in PatientMappingMessage.
    'mapping_id'))

################################################################)

# _Export.EnumerationValue_
EnumerationValueMessage = namedtuple('EnumerationValueMessage', (
    # The original data source (required for looking up
    # enumeration_ids.)
    'origin',

    # An opaque identifier (probably a small integer) for the
    # observation attributes.  I am hoping that those attributes are
    # immutable.  Underlying type is 'bigint'.
    'enumeration_id',

    # A timestamp (probably from DWC or SQL.)
    'timestamp',

    # Sequence number when the observation was made.
    'sequence_number',

    # An opaque identifier (probably a GUID) for a set of
    # simultaneous, related observations (???)
    'compound_value_id',

    # Value, such as a beat label or description of rhythm.
    'value',

    # Should correspond to 'mapping_id' in PatientMappingMessage.
    'mapping_id'))

################################################################

# _Export.NumericValue_
NumericValueMessage = namedtuple('NumericValueMessage', (
    # The original data source (required for looking up
    # numeric_ids.)
    'origin',

    # An opaque identifier (probably a small integer) for the
    # measurement attributes.  I am hoping that these attributes are
    # immutable.  Underlying type is 'bigint'.
    'numeric_id',

    # A timestamp (probably from DWC or SQL.)
    'timestamp',

    # Sequence number when the measurement was made.
    'sequence_number',

    # Supposedly indicates that it's derived from "historic data
    # loaded upon bed association to PIIC iX".
    'is_trend_uploaded',

    # An opaque identifier (probably a GUID) for a set of
    # simultaneous, related measurements.
    'compound_value_id',

    # Measurement value.
    'value',

    # Should correspond to 'mapping_id' in PatientMappingMessage.
    'mapping_id'))

################################################################

# _Export.PatientMapping_
PatientMappingMessage = namedtuple('PatientMappingMessage', (
    # The original data source.
    'origin',

    # An opaque identifier (probably a GUID) for the record.  (This is
    # the 'Id' column in _Export.PatientMapping_.)
    'mapping_id',

    # An opaque identifier (probably a GUID) for the patient.
    'patient_id',

    # A timestamp, origin unknown.  Presumably indicates when the
    # information in this message was updated.
    'timestamp',

    # ???
    'is_mapped',

    # Presumably indicates the original host from which the message
    # was received by the DWC system.
    'hostname'))

# _Export.Patient_
PatientBasicInfoMessage = namedtuple('PatientBasicInfoMessage', (
    # The original data source.
    'origin',

    # An opaque identifier (probably a GUID) for the patient.
    'patient_id',

    # A timestamp, origin unknown.  Presumably indicates when the
    # information in this message was updated.
    'timestamp',

    # Presumably, the name of the bed the patient is assigned to.
    'bed_label',

    # ???
    'alias',

    # Magic number for patient's age category.
    'category',

    # Patient's height.
    'height',

    # Magic number for units of height.
    'height_unit',

    # Patient's weight.
    'weight',

    # Magic number for units of weight.
    'weight_unit',

    # Magic number for units of pressure.  (Why is this here?)
    'pressure_unit',

    # Magic number for whether or not the patient has a pacemaker.
    'paced_mode',

    # ???
    'resuscitation_status',

    # ???
    'admit_state',

    # Presumably, the name of the care unit.
    'clinical_unit',

    # Magic number for sex.
    'gender'))

# _Export.BedTag_
BedTagMessage = namedtuple('BedTagMessage', (
    # The original data source.
    'origin',

    # Name of the bed.
    'bed_label',

    # A timestamp, origin unknown.  Presumably indicates when the
    # information in this message was updated.
    'timestamp',

    # Tag.  What is this?
    'tag'))

# _Export.PatientDateAttribute_
PatientDateAttributeMessage = namedtuple('PatientDateAttributeMessage', (
    # The original data source.
    'origin',

    # An opaque identifier (probably a GUID) for the patient.
    'patient_id',

    # A timestamp, origin unknown.  Presumably indicates when the
    # information in this message was updated.
    'timestamp',

    # Name of the attribute, such as "DOB".
    'name',

    # Value of the attribute.
    'value'))

# _Export.PatientStringAttribute_
PatientStringAttributeMessage = namedtuple('PatientStringAttributeMessage', (
    # The original data source.
    'origin',

    # An opaque identifier (probably a GUID) for the patient.
    'patient_id',

    # A timestamp, origin unknown.  Presumably indicates when the
    # information in this message was updated.
    'timestamp',

    # Name of the attribute.
    'name',

    # Value of the attribute.
    'value'))

################################################################

def bcp_format_message(message):
    """Convert a message to BCP format.

    The argument must be an AlertMessage, BedTagMessage,
    EnumerationValueMessage, NumericValueMessage,
    PatientBasicInfoMessage, PatientDateAttributeMessage,
    PatientMappingMessage, PatientStringAttributeMessage, or
    WaveSampleMessage.

    The result is a byte string which can be written to a file and
    later parsed by freebcp or by the downcast.db.bcp module.

    Note that the result is not always identical to what freebcp
    itself would have produced, since UUIDs are sometimes "natively"
    written as lowercase and sometimes uppercase.
    """
    text = []
    for (field, value) in zip(message._fields, message):
        # ignore the internal "origin" field
        if field == 'origin':
            continue
        # special case for WaveSamples
        if field == 'wave_samples':
            ftext = struct.pack('<I', len(value)) + value
        else:
            if value is None:
                # Null stored as empty field
                ftext = b''
            elif isinstance(value, bool):
                # Booleans stored as '0' or '1'
                ftext = str(int(value)).encode()
            elif isinstance(value, uuid.UUID):
                # UUIDs stored as uppercase
                ftext = str(value).upper().encode()
            else:
                # Other types (str, int, Decimal, T) use the default
                # Python string representation, except that empty
                # strings are stored as b'\0' to distinguish from null
                ftext = str(value).encode() or b'\0'
            ftext += b'\t'
        text.append(ftext)
    if field != 'wave_samples':
        assert ftext[-1:] == b'\t'
        text[-1] = ftext[:-1] + b'\n'
    return b''.join(text)

def bcp_format_description(data_type):
    """Generate a BCP format description for a message type.

    The argument must be one of the classes AlertMessage,
    BedTagMessage, EnumerationValueMessage, NumericValueMessage,
    PatientBasicInfoMessage, PatientDateAttributeMessage,
    PatientMappingMessage, PatientStringAttributeMessage, or
    WaveSampleMessage, or an instance of one of those classes.

    This description can be written to a '.fmt' file and later used by
    freebcp or downcast.db.bcp to parse the data generated by
    bcp_format_message().

    Note that the column names are always written in lowercase.
    """
    # ignore the internal "origin" field
    columns = [f for f in data_type._fields if f != 'origin']
    text = '0.0\n%d\n' % len(columns)
    for (i, field) in enumerate(columns):
        text += str(i + 1)
        # special case for WaveSamples
        if field == 'wave_samples':
            text += ' SYBBINARY 4 -1 "" '
        elif i == len(columns) - 1:
            text += ' SYBCHAR 0 -1 "\\n" '
        else:
            text += ' SYBCHAR 0 -1 "\\t" '
        name = field.replace('_', '')
        # XXX PatientMappingMessage: mapping_id -> id
        text += str(i + 1) + ' ' + name + ' ""\n'
    return text
