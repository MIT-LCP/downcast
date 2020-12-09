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

import re
from datetime import datetime, timedelta, timezone

class T(datetime):
    """Date/time class using MS SQL time string format.

    This class is a wrapper around the standard datetime class, but
    its constructor accepts either a datetime object, or a string in
    the ISO 8601 format used by MS SQL.

    Addition, subtraction, and comparison work as for normal datetime
    objects.  repr and str produce something sensible.
    """

    # Note that the following pattern recognizes several formats:
    #
    #  YYYY-MM-DD HH:MM:SS.SSS +ZZ:ZZ    (MS SQL)
    #  YYYY-MM-DD HH:MM:SS.SSSSSS+ZZ:ZZ  (datetime.__str__)
    #  YYYY-MM-DD HH:MM:SS+ZZ:ZZ         (datetime.__str__ if microseconds = 0)
    #
    # The first format is what should normally be used, but for some
    # reason the timestamps in _phi_time_map files are sometimes
    # written in the latter two formats - this is a bug somewhere in
    # downcast.output.timemap, but for now we need to support the
    # existing _phi_time_map files.

    _pattern = re.compile('\A(\d+)-(\d+)-(\d+)\s+' +
                          '(\d+):(\d+):(\d+)(\.\d+)?\s*' +
                          '([-+])(\d+):(\d+)\Z', re.ASCII)

    def __new__(cls, val, *args):
        # The constructor may be called in various ways:
        #  - T(str), to explicitly convert from a time string
        #  - T(datetime), to explicitly convert from a datetime
        #  - T(int, int, int, int, int, int, int, tzinfo),
        #     used by __add__ and __sub__ in Python 3.8
        #  - T(bytes, tzinfo), used by pickle.loads
        # Only the first two (single argument) forms should be used by
        # applications.

        if args:
            return datetime.__new__(cls, val, *args)

        if isinstance(val, datetime):
            tz = val.tzinfo
            if tz is None:
                raise TypeError('missing timezone')
            return datetime.__new__(
                cls,
                year = val.year,
                month = val.month,
                day = val.day,
                hour = val.hour,
                minute = val.minute,
                second = val.second,
                microsecond = val.microsecond,
                tzinfo = tz)

        m = T._pattern.match(val)
        if m is None:
            raise ValueError('malformed timestamp string %r' % (val,))

        second = int(m.group(6))
        microsecond = round(float(m.group(7) or 0) * 1000000)
        # datetime doesn't support leap seconds, and DWC probably
        # doesn't support them either, but allow for the possibility
        # here just in case.  If there is a leap second, it is
        # silently compressed into the final millisecond of the
        # preceding second; this will result in one or more
        # discontinuities in the record time map.
        if second == 60:
            second = 59
            microsecond = 999000 + microsecond // 1000

        tzs = 1 if m.group(8) == '+' else -1
        tz = timezone(timedelta(hours = tzs * int(m.group(9)),
                                minutes = tzs * int(m.group(10))))

        return datetime.__new__(
            cls,
            year = int(m.group(1)),
            month = int(m.group(2)),
            day = int(m.group(3)),
            hour = int(m.group(4)),
            minute = int(m.group(5)),
            second = second,
            microsecond = microsecond,
            tzinfo = tz)

    def __str__(self):
        tzoffs = round(self.tzinfo.utcoffset(None).total_seconds() / 60)
        (tzh, tzm) = divmod(abs(tzoffs), 60)
        if self.microsecond % 1000 == 0:
            f = '%03d' % (self.microsecond // 1000)
        else:
            f = '%06d' % self.microsecond
        return ('%d-%02d-%02d %02d:%02d:%02d.%s %s%02d:%02d'
                % (self.year, self.month, self.day,
                   self.hour, self.minute, self.second, f,
                   ('-' if tzoffs < 0 else '+'), tzh, tzm))

    def __repr__(self):
        return ('%s(%r)' % (self.__class__.__name__, T.__str__(self)))

    def strftime_local(self, fmt):
        """Format time as a string, using its original timezone."""
        return datetime.strftime(self, fmt)

    def strftime_utc(self, fmt):
        """Convert time to UTC and format as a string."""
        return datetime.strftime(self.astimezone(timezone.utc), fmt)


if not isinstance(T('1800-01-01 00:00:00.000 +00:00') + timedelta(0), T):
    # the following are redundant in Python 3.8
    # also, the above line is a nice sanity check in case Python
    # decides to break this stuff *again*

    def _add_and_convert(a, b):
        return T(datetime.__add__(a, b))
    T.__add__ = _add_and_convert
    T.__radd__ = _add_and_convert

    def _sub_and_convert(a, b):
        d = datetime.__sub__(a, b)
        if isinstance(d, datetime):
            return T(d)
        else:
            return d
    T.__sub__ = _sub_and_convert


def delta_ms(time_a, time_b):
    """Compute the difference between two timestamps in milliseconds."""
    delta = time_a - time_b
    return ((delta.days * 86400 + delta.seconds) * 1000
            + (delta.microseconds // 1000))

very_old_timestamp = T('1800-01-01 00:00:00.000 +00:00')
dwc_epoch = T('2000-01-01 12:00:00.000 +00:00')
