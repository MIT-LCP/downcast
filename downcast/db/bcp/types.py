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

import uuid
import decimal
from datetime import datetime, timezone

from ... import timestamp

class BCPType:
    """
    Base type for database column types.

    Derived classes must implement a function from_bytes(), which
    converts a byte string (retrieved from the BCP file) into the
    appropriate data type.

    A second function from_param() may also be defined, which converts
    a Python value (passed as a parameter to execute()) into the
    appropriate type for comparison.  By default, the identity
    function is used.
    """

    # def from_bytes(b):
    #     return b

    def from_param(p):
        return p

# DB-API types

class BINARY(BCPType):
    """BCP type for a binary column."""
    def from_bytes(b):
        return b

class STRING(BCPType):
    """BCP type for a string column."""
    def from_bytes(b):
        if b == b'\0':
            return ''
        else:
            return b.decode('UTF-8')

class NUMBER(BCPType):
    """BCP type for a real number column."""
    def from_bytes(b):
        return decimal.Decimal(b.decode())

class DATETIME(BCPType):
    """BCP type for a timestamp column."""
    def from_bytes(b):
        return timestamp.T(b.decode())
    def from_param(p):
        return timestamp.T(p)

class ROWID(BCPType):
    """BCP type for a row-ID column."""
    def from_bytes(b):
        return int(b)

# Additional types

class INTEGER(BCPType):
    """BCP type for an integer column."""
    def from_bytes(b):
        return int(b)

class BOOLEAN(BCPType):
    """BCP type for a boolean column."""
    def from_bytes(b):
        return bool(int(b))
    def from_param(p):
        return bool(p)

class UUID(BCPType):
    """BCP type for a UUID column."""
    def from_bytes(b):
        return uuid.UUID(b.decode())
    def from_param(p):
        return uuid.UUID(p)

# DB-API conversion functions

Binary = bytes
Date = datetime.date
Time = datetime.time

def Timestamp(year, month, day, hour, minute, second):
    return datetime(year, month, day, hour, minute, second,
                    tzinfo = timezone.utc)

def TimestampFromTicks(ticks):
    return datetime.fromtimestamp(ticks, tz = timezone.utc)

def DateFromTicks(ticks):
    return TimestampFromTicks(ticks).date()

def TimeFromTicks(ticks):
    return TimestampFromTicks(ticks).time()
