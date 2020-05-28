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
import tempfile

try:
    import setproctitle
    setproctitle = setproctitle.setproctitle
except ImportError:
    def setproctitle(title):
        pass

# fdatasync: ensure data for the given file descriptor is written to disk
# (implemented using fsync if the OS does not appear to support fdatasync)
with tempfile.TemporaryFile() as f:
    try:
        os.fdatasync(f.fileno())
    except Exception:
        fdatasync = os.fsync
    else:
        fdatasync = os.fdatasync

_ascii_substitutions = {
    '\N{HEAVY ASTERISK}': '*',                  # ✱
    '\N{MICRO SIGN}': 'u',                      # µ
    '\N{DEGREE SIGN}': 'deg',                   # °
    '\N{SUBSCRIPT TWO}': '2',                   # ₂
    '\N{SUPERSCRIPT TWO}': '^2',                # ²
    '\N{GREEK CAPITAL LETTER DELTA}': 'Delta',  # Δ
}
for x in list(range(32)) + [127]:
    _ascii_substitutions[x] = ' '
_ascii_substitutions = str.maketrans(_ascii_substitutions)

def string_to_ascii(string):
    """
    Convert various characters to approximate ASCII equivalents.

    >>> string_to_ascii('✱✱✱ VTach')
    '*** VTach'
    >>> string_to_ascii('µV')
    'uV'
    >>> string_to_ascii('°C')
    'degC'
    >>> string_to_ascii('SpO₂')
    'SpO2'
    >>> string_to_ascii('ml/m²')
    'ml/m^2'
    >>> string_to_ascii('ΔTemp')
    'DeltaTemp'
    """
    return string.translate(_ascii_substitutions)
