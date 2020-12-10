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
from ..util import fdatasync

class TimeMap:
    """
    Object that tracks the mapping between time and sequence number.

    In general, sequence numbers provide a reliable measurement of
    time; wall-clock timestamps do not.

    (For example, two events whose sequence numbers differ by
    1,000,000 are exactly twice as far apart as two events whose
    sequence numbers differ by 500,000.  However, two events whose
    wall-clock timestamps differ by 1,000 seconds might be anywhere
    from 970 to 1,030 seconds apart.)

    This object aggregates the available information concerning the
    mapping (which is not necessarily injective in either direction)
    between sequence number and timestamp, so that given an arbitrary
    timestamp, it is possible to determine the most likely sequence
    number at which that timestamp would have been generated.
    """

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
            fdatasync(f.fileno())
        os.rename(tmpfname, fname)

    def set_time(self, seqnum, time):
        """
        Add a reference timestamp to the map.

        This indicates that we know (from a reliable source, such as a
        wave sample message) the exact wall-clock time at a given
        sequence number.

        Given this information, we can infer what the wall-clock time
        must have been at other moments in time, so long as the wall
        clock is not adjusted.

        This information is treated as trustworthy and will be saved
        to the time map file when write() is called.
        """
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

    def add_time(self, time):
        """
        Add a non-reference timestamp to the map.

        This indicates that we have observed the given wall-clock time
        (for example, it is used as the timestamp of a numeric or
        alert message), but we do not yet know precisely when that
        timestamp occurred.

        This information is not saved in the time map file, but is
        used by resolve_gaps() to refine the time map.

        This function should be called after all reference timestamps
        have been recorded using set_time().
        """
        for e in self.entries:
            start = e[2] + timedelta(milliseconds = e[0])
            if time < start:
                e[3].add(time)
                return
            end = e[2] + timedelta(milliseconds = e[1])
            if time <= end:
                return

    def get_seqnum(self, time, limit = None):
        """
        Guess the sequence number corresponding to a wall-clock time.

        limit should be the latest possible value (inclusive) for this
        sequence number.  Typically, if the message sequence number is
        N, then it should be impossible for any event to have occurred
        at time greater than (N + 5120).

        If no information is available, this will return None.
        """

        if not self.entries:
            return None

        if limit is None:
            limit = self.entries[-1][1]

        # If this timestamp falls within a known interval - there is
        # an instant at which we know the system clock would have
        # displayed that value - then choose the latest such instant
        # that is before or equal to 'limit'.
        possible_sn = []
        best_known = None
        for (start, end, base, _) in self.entries:
            sn = delta_ms(time, base)
            possible_sn.append((sn, end))
            if start <= sn <= end and sn <= limit:
                best_known = sn
        if best_known is not None:
            return best_known

        # Otherwise, take the earliest interval for which this
        # timestamp would appear to be in the past.  (So, if the
        # system clock never displayed this timestamp, then translate
        # according to the next reference timestamp *after* this
        # point.  If the system clock displayed this timestamp
        # multiple times, but all of those occurred after 'limit',
        # then choose the earliest.)
        for (sn, interval_end) in possible_sn:
            if sn <= interval_end:
                return sn

        # Otherwise, the timestamp occurs in the future; extrapolate
        # from the *last* reference timestamp.
        return possible_sn[-1][0]

    def get_time(self, seqnum):
        """
        Guess the wall-clock time corresponding to a sequence number.

        If no information is available, this will return None.
        """
        best_time = None
        best_delta = None
        for (start, end, base, _) in self.entries:
            delta = max(start - seqnum, seqnum - end)
            if best_delta is None or delta < best_delta:
                best_time = base + timedelta(milliseconds = seqnum)
                best_delta = delta
        return best_time

    def resolve_gaps(self):
        """
        Refine the time map based on all available information.

        The wall clock may be adjusted at any time during the record;
        in general, there is no way to know exactly when this happens.
        When it does, two consecutive reference timestamps will
        disagree; for example, we might have

          sequence number     timestamp
          500000000000        2015-11-05 12:53:20.000 +00:00
          500000005120        2015-11-05 12:53:27.120 +00:00

        This tells us that, at some time between those two events, the
        wall clock was adjusted forward by two seconds.  If we then
        see:

          (unknown)           2015-11-05 12:53:23.800 +00:00

        we can't tell whether that occurs 3.8 seconds after event #1,
        or 3.32 seconds before event #2.  However, if we also see:

          (unknown)           2015-11-05 12:53:21.900 +00:00

        we can deduce that the two-second adjustment could not
        possibly have occurred between events #1 and #4, nor between
        events #4 and #3, and thus it must have been between events #3
        and #2; so event #4 must have occurred at sequence number
        500000001900, and event #3 at 500000003800.

        In ambiguous cases, our best guess is that the adjustment
        occurred between the most distant pair of timestamps - if we
        only have events #1-#3 above, then all we can say is that it's
        more likely to have a 3.32-second interval with no events,
        than to have a 3.8-second interval with no events, and thus
        the clock adjustment is more likely to have occurred between
        events #1 and #3.
        """
        p = None
        for n in self.entries:
            if p and n[3]:
                gapstart = p[2] + timedelta(milliseconds = p[1])
                gapend = n[2] + timedelta(milliseconds = n[0])
                n[3].add(gapstart)
                n[3].add(gapend)
                best = (timedelta(0), gapstart)
                for d in _differences(sorted(n[3])):
                    best = max(best, d)
                tbefore = best[1]
                tafter = best[1] + best[0]
                snp = delta_ms(tbefore, p[2])
                snn = delta_ms(tafter, n[2])
                self.set_time(snp, tbefore)
                self.set_time(snn, tafter)
            p = n

def _differences(k):
    i = iter(k)
    try:
        prev = next(i)
    except StopIteration:
        return
    for cur in i:
        yield (cur - prev, prev)
        prev = cur
