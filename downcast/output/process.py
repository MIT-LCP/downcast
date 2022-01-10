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
import sys
import cProfile
from multiprocessing import Process

from ..util import setproctitle

class WorkerProcess(Process):
    def __init__(self, name = None, **kwargs):
        Process.__init__(self, name = name, **kwargs)

    def run(self):
        name = self.name
        if name is not None:
            setproctitle('downcast:%s' % (name,))

        # Invoke the target function, with profiling if enabled
        pf = os.environ.get('DOWNCAST_PROFILE_OUT', None)
        if pf is not None and name is not None:
            pf = '%s.%s' % (pf, name)
            cProfile.runctx('Process.run(self)', globals(), locals(), pf)
        else:
            Process.run(self)
