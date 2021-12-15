#
# downcast - tools for unpacking patient data from DWC
#
# Copyright (c) 2021 Laboratory for Computational Physiology
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

import io
import os
import tempfile

_dev_fd_yields_independent_ofd = False
with tempfile.TemporaryFile() as f1:
    f1.write(b'what hath god wrought')
    f1.flush()
    try:
        with open('/dev/fd/%d' % f1.fileno(), 'rb') as f2:
            if (os.path.samefile(f1.fileno(), f2.fileno())
                and (os.lseek(f1.fileno(), 0, os.SEEK_CUR)
                     != os.lseek(f2.fileno(), 0, os.SEEK_CUR))):
                _dev_fd_yields_independent_ofd = True
    except OSError:
        pass

def open_copy(fileobj, *args, **kwargs):
    """
    Open a new file object that refers to the same underlying file.

    The input must be a Python file object.  The result will be an
    independent file object that refers to the same file.

    If the operating system provides a /dev/fd filesystem, and that
    filesystem allows creating independent OFDs, then this can be done even
    if the original file has been deleted or renamed.

    If the operating system *doesn't* provide /dev/fd, or if /dev/fd
    uses dup semantics, this will attempt to reopen the original
    filename (fileobj.path) instead, which will fail if the original
    file has been deleted or renamed.
    """
    if isinstance(fileobj, io.TextIOWrapper):
        fileobj = fileobj.buffer
    if isinstance(fileobj, io.BufferedReader):
        fileobj = fileobj.raw
    if not isinstance(fileobj, io.FileIO):
        raise TypeError('not a native file object')

    if _dev_fd_yields_independent_ofd:
        return open('/dev/fd/%d' % fileobj.fileno(), *args, **kwargs)
    else:
        oldpath = fileobj.name
        oldfd = fileobj.fileno()
        newfile = open(oldpath, *args, **kwargs)
        try:
            if os.path.samefile(oldfd, newfile.fileno()):
                return newfile
            else:
                raise FileNotFoundError(0, 'File has been renamed', oldpath)
        except OSError:
            newfile.close()
            raise
