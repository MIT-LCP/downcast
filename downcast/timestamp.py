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

    # Note the unusual format of the timezone which (for some
    # braindead reason) means we can't use datetime.strptime or
    # datetime.strftime.  It's especially braindead given that
    # datetime.__str__ uses a very similar format.
    _pattern = re.compile('\A(\d+)-(\d+)-(\d+)\s+' +
                          '(\d+):(\d+):(\d+)(\.\d+)\s*' +
                          '([-+])(\d+):(\d+)\Z', re.ASCII)

    def __new__(cls, val):
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
            raise ValueError('malformed timestamp string')

        second = int(m.group(6))
        microsecond = round(float(m.group(7)) * 1000000)
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

    def __add__(a, b):
        return T(datetime.__add__(a, b))

    def __sub__(a, b):
        d = datetime.__sub__(a, b)
        if isinstance(d, datetime):
            return T(d)
        else:
            return d

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

very_old_timestamp = T('1800-01-01 00:00:00.000 +00:00')
