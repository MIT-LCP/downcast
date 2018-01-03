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

# Note that 'enumeration_id', 'numeric_id', and 'wave_id' are
# deliberately omitted.  Contents of these attribute structures should
# be fully anonymized.

# _Export.Enumeration_
EnumerationAttr = namedtuple('EnumerationAttr', (
    # Magic number for... something.  See
    # System_Parameter-Alerts_Table_Ed_2_-_PIIC_iX_Rel_B.00.xlsx
    # (Parameters).  Underlying type is 'bigint'.
    'base_physio_id',

    # Magic number for the enumeration.  See
    # System_Parameter-Alerts_Table_Ed_2_-_PIIC_iX_Rel_B.00.xlsx
    # (Parameters).  Underlying type is 'bigint'.
    'physio_id',

    # Description of the enumeration, such as 'Annot' or 'RhySta'.
    'label',

    # Undocumented magic number.  Underlying type is 'bigint'.
    'value_physio_id',

    # Supposedly indicates if observation is aperiodic.
    # Seems to be 0 even for 'Annot'.
    'is_aperiodic',

    # Indicates if observation is manually entered, I guess???
    'is_manual',

    # Magic number indicating whether observation is valid????
    'validity',

    # Magic number for the units of measurement.  See
    # System_Parameter-Alerts_Table_Ed_2_-_PIIC_iX_Rel_B.00.xlsx
    # (Units-Of-Measure).  Underlying type is 'bigint'.
    'unit_code',

    # Units of measurement, if that makes any sense (current enums say
    # 'Unknwn'.)  (What IS an "enumeration", if not something that
    # lacks units of measurement?)
    'unit_label',

    # Color to use for displaying enumeration values, represented as
    # 0xAARRGGBB, reinterpreted as a signed 32-bit integer.
    'color'))

undefined_enumeration = EnumerationAttr(*[None]*10)

# _Export.Numeric_
NumericAttr = namedtuple('NumericAttr', (
    # Magic number for... something.  Underlying type is 'bigint'.
    'base_physio_id',

    # Magic number for the "category" of numeric.  See
    # System_Parameter-Alerts_Table_Ed_2_-_PIIC_iX_Rel_B.00.xlsx
    # (Parameters? or Calculations?)  Underlying type is 'bigint'.
    'physio_id',

    # Description of the "category" of numeric (such as 'NBP'.)
    'label',

    # Indicates that the measurement is aperiodic (like NBP), rather
    # than periodic (like HR).
    'is_aperiodic',

    # Units of measurement.
    'unit_label',

    # Magic number indicating whether measurement is valid????
    'validity',

    # Lower alarm threshold (?!)
    'lower_limit',

    # Upper alarm threshold (?!)
    'upper_limit',

    # Indicates that threshold(?) alarms are disabled (?!)
    'is_alarming_off',

    # Magic number for the specific numeric.  See
    # System_Parameter-Alerts_Table_Ed_2_-_PIIC_iX_Rel_B.00.xlsx
    # (Parameters? or Calculations?)  Underlying type is 'bigint'.
    'sub_physio_id',

    # Description of the specific numeric (such as 'NBPs'.)
    'sub_label',

    # Color to use for displaying numeric values, represented as
    # 0xAARRGGBB, reinterpreted as a signed 32-bit integer.
    'color',

    # Indicates if value is manually entered, I guess???
    'is_manual',

    # Number of values belonging to the compound value???
    'max_values',

    # Number of decimal places to be displayed (?)
    'scale'))

undefined_numeric = NumericAttr(*[None]*15)

# _Export.Wave_
WaveAttr = namedtuple('WaveAttr', (
    # Magic number for the "category" of waveform.  See
    # System_Parameter-Alerts_Table_Ed_2_-_PIIC_iX_Rel_B.00.xlsx
    # (Parameters).  Underlying type is 'bigint'.
    'base_physio_id',

    # Magic number for the specific waveform.  See
    # System_Parameter-Alerts_Table_Ed_2_-_PIIC_iX_Rel_B.00.xlsx
    # (Parameters).  Underlying type is 'bigint'.
    'physio_id',

    # Description of the waveform.
    'label',

    # 0 = Primary, 1 = Secondary ???
    'channel',

    # Presumably, number of seqnum ticks per sample.
    'sample_period',

    # Indicates the waveform should be displayed with lower time
    # resolution than usual.
    'is_slow_wave',

    # Indicates that the waveform is "derived". ???
    'is_derived',

    # Color to use for displaying the waveform, represented as
    # 0xAARRGGBB, reinterpreted as a signed 32-bit integer.
    'color',

    # Low/high cutoff frequency of the input bandpass filter.
    'low_edge_frequency',
    'high_edge_frequency',

    # Range of sample values.
    'scale_lower',
    'scale_upper',

    # Two reference sample values.
    'calibration_scaled_lower',
    'calibration_scaled_upper',

    # Physical values corresponding to the two reference sample
    # values.
    'calibration_abs_lower',
    'calibration_abs_upper',

    # Magic number indicating how signal is calibrated (???)
    'calibration_type',

    # Units of measurement.
    'unit_label',

    # Magic number for the units of measurement.  See
    # System_Parameter-Alerts_Table_Ed_2_-_PIIC_iX_Rel_B.00.xlsx
    # (Units-Of-Measure).  Underlying type is 'bigint'.
    'unit_code',

    # Magic number indicating electrode placement (???)
    'ecg_lead_placement'))

undefined_wave = WaveAttr(*[None]*20)

# "Parameters" table
PhysioIDAttr = namedtuple('PhysioIDAttr', (
    # I guess this is a standard code of some sort (comment says HL7)?
    'mdil_code',

    # The Philips internal identifier for the signal/parameter, used
    # in various structures.  (These don't appear related to the
    # "StardateNom" numbering system used in DataExport and RDE.)
    'physio_id',

    # Short description of the signal/parameter.
    'label',

    # Verbose description of the signal/parameter.
    'description',

    # I guess this is another standard code of some sort?  Often this
    # equals the PhysioId.
    'mdc_code',

    # I guess this is another standard code, in this case a symbolic
    # name.
    'mdc_label',

    # Defines how the physioid is used, I guess: "wave", "numeric",
    # "numeric/wave", "setting/numeric", or "string/enumeration".
    # Maybe other possibilities, who knows?
    'type',

    # ???
    'hl7_outbound',

    # ???
    'data_warehouse_connect'))

# "Units-Of-Measure" table
UnitAttr = namedtuple('UnitAttr', (
    # I guess this is a standard code of some sort (comment says HL7)?
    'mdil_code',

    # The Philips internal identifier for the unit, used in various
    # structures.
    'unit_code',

    # Abbreviation for the unit.  Not typographically consistent
    # ("°F", "/mm³", "cmH2O/l/s", "1/nl", ...)
    'label',

    # I guess this is another standard code of some sort?  Often this
    # equals the unit_code.
    'mdc_code',

    # I guess this is another standard code, in this case a symbolic
    # name.
    'mdc_label',

    # Verbose description, even more typographically inconsistent than
    # the label.
    'description'))
