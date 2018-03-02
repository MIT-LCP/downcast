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
import csv
import bisect
import logging
from datetime import timedelta

from ..timestamp import T, delta_ms

class TimeMap:
    """Object that tracks the mapping between time and sequence number."""

    def __init__(self, record_id):
        self.entries = []
        self.record_id = record_id

    def read(self, path, name):
        """Read a time map file."""
        fname = os.path.join(path, name)
        try:
            with open(fname, 'rt', encoding = 'UTF-8') as f:
                for row in csv.reader(f):
                    start = int(row[0])
                    end = int(row[1])
                    baset = T(row[2])
                    self.entries.append([start, end, baset, set()])
        except FileNotFoundError:
            pass
        self.entries.sort()

    def write(self, path, name):
        """Write a time map file."""
        fname = os.path.join(path, name)
        tmpfname = os.path.join(path, '_' + name + '.tmp')
        with open(tmpfname, 'wt', encoding = 'UTF-8') as f:
            w = csv.writer(f)
            for e in self.entries:
                w.writerow(e[0:3])
            f.flush()
            os.fdatasync(f.fileno())
        os.rename(tmpfname, fname)

    def set_time(self, seqnum, time):
        """Add a wall-clock time reference to the map."""

        baset = time - timedelta(milliseconds = seqnum)

        # i = index of the first span that begins at or after seqnum
        i = bisect.bisect_right(self.entries, [seqnum])
        p = self.entries[i-1:i]
        n = self.entries[i:i+1]

        # If this sequence number falls within an existing span,
        # verify that baset is what we expect
        if p and seqnum <= p[0][1]:
            if baset != p[0][2]:
                logging.warning('conflicting timestamps at %d in %s'
                                % (seqnum, self.record_id))
        elif n and seqnum >= n[0][0]:
            if baset != n[0][2]:
                logging.warning('conflicting timestamps at %d in %s'
                                % (seqnum, self.record_id))

        # If this sequence number falls close to the start or end of
        # an existing span that has the same baset value (close enough
        # that we assume there could not have been more than one clock
        # adjustment), then extend the existing span(s)
        elif p and p[0][2] == baset and seqnum - p[0][1] < 30000:
            p[0][1] = seqnum
            if n and n[0][2] == baset and n[0][0] - seqnum < 30000:
                n[0][0] = p[0][0]
                del self.entries[i-1]
        elif n and n[0][2] == baset and n[0][0] - seqnum < 30000:
            n[0][0] = seqnum

        # Otherwise, define a new span
        else:
            self.entries.insert(i, [seqnum, seqnum, baset, set()])
