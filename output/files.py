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
import mmap

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

class ArchiveBinaryFile:
    def __init__(self, filename, window_size = None):
        # Open the file R/W and create if missing, never truncate
        self.fd = os.open(filename, os.O_RDWR|os.O_CREAT, 0o666)

        self.current_size = os.lseek(self.fd, 0, os.SEEK_END)
        self.real_size = self.current_size

        self.window_size = mmap.PAGESIZE * 2
        if window_size is not None:
            while self.window_size < window_size:
                self.window_size *= 2

        self.map_start = self.map_end = 0
        self.map_buffer = None

    def _map_range(self, start, end):
        if end < self.map_start or start >= self.map_end:
            start -= start % mmap.PAGESIZE
            if end < start + self.window_size:
                end = start + self.window_size
            else:
                end += mmap.PAGESIZE - (end % mmap.PAGESIZE)
            if end > self.current_size:
                os.ftruncate(self.fd, end)
                self.current_size = end
            self.map_buffer = mmap.mmap(self.fd, end - start, offset = start)
            self.map_start = start
            self.map_end = end

    def size(self):
        return self.real_size

    def truncate(self, size):
        self.real_size = size

    def write(self, pos, data, mask = None):
        end = pos + len(data)
        if end > self.real_size:
            self.real_size = end
        self._map_range(pos, end)
        i = pos - self.map_start
        if mask is None:
            self.map_buffer[i : i + len(data)] = data
        else:
            for j in range(len(data)):
                self.map_buffer[i + j] = ((self.map_buffer[i + j] & ~mask[j])
                                          | (data[j] & mask[j]))

    def flush(self):
        self.map_start = self.map_end = 0
        if self.map_buffer is not None:
            self.map_buffer.close()
            self.map_buffer = None
        if self.real_size != self.current_size:
            os.ftruncate(self.fd, self.real_size)
            self.current_size = self.real_size
        os.fdatasync(self.fd)

    def close(self):
        self.flush()
        os.close(self.fd)
