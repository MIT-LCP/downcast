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
import errno

class ArchiveLogFile:
    def __init__(self, filename):
        # Open file
        self.fp = open(filename, 'a+b')

        # Check if file ends with \n; if not, append a marker to
        # indicate the last line is invalid
        try:
            self.fp.seek(-1, os.SEEK_END)
        except OSError as e:
            if e.errno == errno.EINVAL:
                return
            else:
                raise
        c = self.fp.read(1)
        if c != b'\n' and c != b'':
            self.fp.write(b'\030\r####\030\n')

    def append(self, msg):
        self.fp.write(msg.encode('UTF-8'))
        self.fp.write(b'\n')

    def flush(self):
        self.fp.flush()
        os.fdatasync(self.fp.fileno())

    def close(self):
        self.flush()
        self.fp.close()
