#
# dwctimeconv - convert between time formats in a converted record
#
# Copyright (c) 2020 Laboratory for Computational Physiology
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

import argparse
import json
import os
import re
import sys

from .timestamp import T
from .output.archive import ArchiveRecord

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--record', metavar = 'PATH', default = '.')
    p.add_argument('timestamps', metavar = 'TIMESTAMP', nargs = '+')
    opts = p.parse_args()

    rec = ArchiveRecord(path = opts.record, servername = 'unknown',
                        record_id = os.path.basename(opts.record),
                        datestamp = 'unknown')
    seqnum0 = rec.seqnum0()

    for ts in opts.timestamps:
        if re.fullmatch('S\d+', ts):
            # sequence number
            seqnum = int(ts[1:])
            time = rec.time_map.get_time(seqnum)
            if seqnum0 is not None:
                counter = seqnum - seqnum0
        elif re.fullmatch('c\d+', ts):
            # counter value
            counter = int(ts[1:])
            if seqnum0 is not None:
                seqnum = seqnum0 + counter
                time = rec.time_map.get_time(seqnum)
        else:
            # wall clock timestamp
            try:
                time = T(ts)
            except ValueError:
                sys.stderr.write('%s: invalid argument: %s\n' % (sys.argv[0], ts))
                sys.stderr.write('valid timestamp formats:\n')
                sys.stderr.write('  YYYY-MM-DD HH:MM:SS.SSS +ZZ:ZZ\n')
                sys.stderr.write('  S#########  (DWC sequence number)\n')
                sys.stderr.write('  c#########  (WFDB counter value)\n')
                sys.exit(1)

            seqnum = rec.time_map.get_seqnum(time)
            if seqnum0 is not None:
                counter = seqnum - seqnum0

        if time is None:
            time_str = '-'
        else:
            time_str = str(time)

        if seqnum is None:
            seqnum_str = '-'
        else:
            seqnum_str = 'S%s' % seqnum

        if counter is None:
            counter_str = '-'
        else:
            counter_str = 'c%s' % counter

        print('%-24s\t%-8s\t%-8s' % (time_str, seqnum_str, counter_str))
