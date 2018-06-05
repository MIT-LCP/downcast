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

import heapq

class ArchiveLogReader:
    """Class for reading log entries from a mostly-sorted input file.

    Each line in the input file is either a data record, a timestamp,
    or a sequence number.

    Timestamps are written as a decimal integer (interpreted as a
    string of digits, giving the UTC year, month, day, hour, minute,
    second, and microsecond).

    Sequence numbers are written as the letter 'S' followed by a
    decimal integer (interpreted as the number of milliseconds since
    the epoch.)

    All other lines in the file are treated as data records, and are
    associated with the preceding timestamp and sequence number (thus
    allowing basic compression, while keeping the file format
    extremely simple.)

    When reading the file, data records are returned in order (sorting
    first by sequence number, then by timestamp, then by order in the
    input file.)  This will be done efficiently if the input file is
    mostly sorted to begin with (and less efficiently otherwise.)

    No attempt is made to remove duplicate or invalid records - this
    must be done by the caller.

    If the file is modified after being opened, then garbage in,
    garbage out.
    """

    def __init__(self, filename, allow_missing = False):
        # Open the file
        try:
            fp = open(filename, 'rb')
        except FileNotFoundError:
            if allow_missing:
                self._fp = None
                self._subsequences = None
                return
            else:
                raise
        self._fp = fp
        self._subsequences = None

    def close(self):
        if self._fp:
            self._fp.close()
        self._subsequences = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def missing(self):
        return (self._fp is None)

    def unsorted_items(self):
        fp = self._fp
        if not fp:
            return
        fp.seek(0)
        subseq = []
        prev_t = None
        sn = ts = 0
        t = (sn, ts)
        for line in fp:
            try:
                if line[0] == 83: # ASCII 'S'
                    sn = int(line[1:])
                    t = (sn, ts)
                else:
                    ts = int(line)
                    t = (sn, ts)
            except ValueError:
                yield (sn, ts, line)
                if not subseq or t < prev_t:
                    fpos = fp.tell() - len(line)
                    subseq.append((sn, ts, fpos))
                prev_t = t
        heapq.heapify(subseq)
        self._subsequences = subseq

    def sorted_items(self):
        if self._subsequences is None:
            for _ in self.unsorted_items():
                pass

        fp = self._fp
        subseq = self._subsequences
        self._subsequences = None
        while subseq:
            # Begin reading the earliest subsequence
            p = heapq.heappop(subseq)
            (sn, ts, fpos) = prev_p = p
            fp.seek(fpos)

            for line in fp:
                try:
                    if line[0] == 83: # ASCII 'S'
                        sn = int(line[1:])
                        p = (sn, ts, fpos)
                    else:
                        ts = int(line)
                        p = (sn, ts, fpos)
                except ValueError:
                    if p < prev_p:
                        # reached end of subsequence
                        break
                    # Note that because the subsequences are disjoint,
                    # this comparison is valid even though fpos is not
                    # continuously updated.
                    elif subseq and p > subseq[0]:
                        # switch to other subsequence
                        fpos = fp.tell() - len(line)
                        p = heapq.heapreplace(subseq, (sn, ts, fpos))
                        (sn, ts, fpos) = prev_p = p
                        fp.seek(fpos)
                        next_p = subseq[0]
                    else:
                        # continue with current subsequence
                        yield (sn, ts, line)
                        prev_p = p
