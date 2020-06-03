#
# downcast - tools for unpacking patient data from DWC
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

import collections
import math
import os
import re

from ..util import fdatasync

def str_to_version(s):
    """Split a version string into a tuple of integers."""
    return tuple(int(n) for n in s.split('.'))

def version_to_str(v):
    """Convert a tuple of integers into a version string."""
    return '.'.join(str(n) for n in v)

class SignalInfo:
    def __init__(self, fname = None, fmt = 0, spf = 1, skew = 0, start = 0,
                 gain = 0, baseline = 0, units = None, adcres = 0, adczero = 0,
                 initval = 0, cksum = 0, bsize = 0, desc = None):
        self.fname = fname
        self.fmt = fmt
        self.spf = spf
        self.skew = skew
        self.start = start
        self.gain = gain
        self.baseline = baseline
        self.units = units
        self.adcres = adcres
        self.adczero = adczero
        self.initval = initval
        self.cksum = cksum
        self.bsize = bsize
        self.desc = desc

class SegmentHeader:
    """Class for reading and writing WFDB segment header files.

    This implements a subset of the WFDB header format which should be
    adequate for reading and writing segment headers created by
    downcast.
    """
    def __init__(self, path=None):
        self.name = None
        self.ffreq = None
        self.cfreq = None
        self.basecount = None
        self.nframes = None
        self.signals = []
        self.info = []
        self.min_version = ()
        if path is not None:
            self.read(path)

    def read(self, path):
        """Read signal information from a header file."""
        with open(path, 'rt', encoding = 'UTF-8') as hf:
            self.min_version = ()
            for line in hf:
                if line.startswith('#wfdb'):
                    self.min_version = str_to_version(line[5:].strip())
                    continue
                fields = line.split()
                if not fields or fields[0].startswith('#'):
                    continue
                if '/' in fields[0] or len(fields) < 4:
                    raise ValueError('unsupported header format')

                self.name = fields[0]
                nsig = int(fields[1])
                (ffreq, cfreq, basecount) = re.fullmatch(
                    r'([^/()]+)(?:/([^/()]+)(?:\(([^/()]+)\))?)?',
                    fields[2]).groups()
                self.ffreq = float(ffreq)
                self.cfreq = float(cfreq or ffreq)
                self.basecount = float(basecount or 0)
                self.nframes = int(fields[3])
                break

            self.signals = []
            self.info = []
            for line in hf:
                fields = line.rstrip('\r\n').split(maxsplit = 8)
                if not fields:
                    continue
                if fields[0].startswith('#'):
                    self.info.append(line[line.index('#')+1:].rstrip('\r\n'))
                    continue
                if len(fields) != 9:
                    raise ValueError('unsupported header format')
                sig = SignalInfo()
                sig.fname = fields[0]
                (fmt, spf, skew, start) = re.fullmatch(
                    r'([0-9]+)(?:x([0-9]+)|:([0-9]+)|\+([0-9]+))*',
                    fields[1]).groups()
                sig.fmt = int(fmt)
                sig.spf = int(spf or 1)
                sig.skew = int(skew or 0)
                sig.start = int(start or 0)
                (gain, baseline, units) = re.fullmatch(
                    r'([^()/]+)(?:\(([^()/]+)\))?(?:/(.*))?',
                    fields[2]).groups()
                sig.gain = float(gain)
                sig.units = units
                sig.adcres = int(fields[3])
                sig.adczero = int(fields[4])
                sig.baseline = int(baseline or sig.adczero)
                sig.initval = int(fields[5])
                sig.cksum = int(fields[6])
                sig.bsize = int(fields[7])
                sig.desc = fields[8]
                self.signals.append(sig)
                self.info = []

            if len(self.signals) != nsig:
                raise ValueError('wrong number of signals')

    def write(self, path, fsync = True):
        """Write signal information to a header file."""
        recname = os.path.basename(path)
        if not recname.endswith('.hea') or recname == '.hea':
            raise ValueError('invalid header file name')
        recname = recname[:-4]

        with open(path, 'wt', encoding = 'UTF-8') as hf:
            if self.min_version:
                hf.write('#wfdb %s\n' % version_to_str(self.min_version))
            hf.write('%s %d %.16g' % (recname, len(self.signals), self.ffreq))
            if self.cfreq != self.ffreq or self.basecount != 0:
                hf.write('/%.16g' % (self.cfreq,))
                if self.basecount != 0:
                    hf.write('(%.16g)' % (self.basecount,))
            if self.nframes is not None:
                hf.write(' %d' % (self.nframes,))
            hf.write('\n')

            for sig in self.signals:
                hf.write('%s %d' % (sig.fname, sig.fmt))
                if sig.spf != 1:
                    hf.write('x%d' % (sig.spf,))
                if sig.skew != 0:
                    hf.write(':%d' % (sig.skew,))
                if sig.start != 0:
                    hf.write(':%d' % (sig.start,))
                hf.write(' %.16g' % (sig.gain,))
                if sig.baseline != sig.adczero:
                    hf.write('(%d)' % (sig.baseline,))
                if sig.units is not None:
                    hf.write('/%s' % (sig.units,))
                hf.write(' %d %d %d %d %d %s\n'
                         % (sig.adcres, sig.adczero, sig.initval,
                            sig.cksum, sig.bsize, sig.desc))
            for info in self.info:
                hf.write('#%s\n' % (info,))

            if fsync:
                hf.flush()
                fdatasync(hf.fileno())

def _default_siginfo_sort_key(siginfo):
    if siginfo.units == 'mV':
        return (0, siginfo.desc)
    elif siginfo.units == 'mmHg':
        return (1, siginfo.desc)
    else:
        return (2, siginfo.desc)

def join_segments(record_header, segment_headers, layout_suffix = '_layout',
                  sort_key = None, fsync = True):
    """Join a sequence of segments into a multi-segment record.

    All of the segments must have the same frame frequency and the
    same counter frequency, and will be aligned according to their
    base counter values.  (For example, if the counter frequency is
    1000 Hz, and there are two contiguous segments that are each one
    hour long, the first segment should have a base counter value of 0
    and the second should have a base counter value of 3600000.)

    Each named signal must have the same physical units, the same
    skew, and the same number of samples per frame in every segment
    where the signal appears.
    """

    recdir = os.path.dirname(record_header)
    recname = os.path.basename(record_header)
    if not recname.endswith('.hea') or recname == '.hea':
        raise ValueError('invalid header file name')
    recname = recname[:-4]

    if not segment_headers:
        raise ValueError('no segments provided')

    if sort_key is None:
        sort_key = _default_siginfo_sort_key

    layout_name = recname + layout_suffix

    ffreq = None
    cfreq = None
    signals = collections.OrderedDict()
    segments = [(layout_name, 0)]
    basecount = 0
    end = 0
    prevsegment = '(start of record)'
    min_version = ()

    # Read all segment headers, check consistency, and generate a
    # dictionary of all available signals.
    for h in segment_headers:
        seg = SegmentHeader(h)
        if ffreq is None:
            ffreq = seg.ffreq
            cfreq = seg.cfreq
        else:
            if ffreq != seg.ffreq:
                raise ValueError('ffreq mismatch in segment %s' % (seg.name,))
            if cfreq != seg.cfreq:
                raise ValueError('cfreq mismatch in segment %s' % (seg.name,))
        t = int((seg.basecount - basecount) * ffreq / cfreq)
        if t < end:
            raise ValueError('segment %s overlaps with %s'
                             % (seg.name, prevsegment))
        elif t > end:
            segments.append(('~', t - end))
        segments.append((seg.name, seg.nframes))
        prevsegment = seg.name
        end = t + seg.nframes

        min_version = max(min_version, seg.min_version)

        for sig in seg.signals:
            if sig.spf > 1 or sig.fname != seg.signals[0].fname:
                min_version = max(min_version, (10, 6))
            if sig.skew != 0:
                min_version = max(min_version, (10, 7))

            if sig.adcres > 0:
                adu1 = sig.adczero - (1 << (sig.adcres - 1))
                adu2 = sig.adczero + (1 << (sig.adcres - 1)) - 1
            else:
                adu1 = adu2 = sig.adczero
            if adu1 == -32768:
                adu1 = -32767

            phys1 = (adu1 - sig.baseline) / sig.gain
            phys2 = (adu2 - sig.baseline) / sig.gain
            sig._minphys = min(phys1, phys2)
            sig._maxphys = max(phys1, phys2)
            sig.gain = abs(sig.gain)

            oldsig = signals.get(sig.desc)
            if oldsig is None:
                signals[sig.desc] = sig
            else:
                if oldsig.spf != sig.spf:
                    raise ValueError('spf mismatch in %s' % (sig.desc))
                if oldsig.skew != sig.skew:
                    raise ValueError('skew mismatch in %s' % (sig.desc))
                if oldsig.units != sig.units:
                    raise ValueError('units mismatch in %s' % (sig.desc))
                oldsig.gain = max(oldsig.gain, sig.gain)
                oldsig._minphys = min(oldsig._minphys, sig._minphys)
                oldsig._maxphys = max(oldsig._maxphys, sig._maxphys)

    layout = SegmentHeader()
    layout.ffreq = ffreq
    layout.cfreq = cfreq
    layout.basecount = 0
    layout.nframes = 0
    layout.signals = list(signals.values())
    layout.signals.sort(key = sort_key)

    for sig in layout.signals:
        sig.fname = '~'
        sig.fmt = 0
        sig.bsize = 0
        sig.adczero = 0
        sig.cksum = 0
        sig.initval = 0

        vrange = (sig._maxphys - sig._minphys) * sig.gain
        sig.adcres = math.ceil(math.log2(vrange + 1))
        while sig.adcres > 31:
            sig.gain /= 2
            sig.adcres -= 1
        if sig.adcres <= 16:
            sig.adczero = 0
        else:
            sig.adczero = (1 << (sig.adcres - 1))
        vmin = sig.adczero - (1 << (sig.adcres - 1))
        vmax = vmin + (1 << sig.adcres) - 1

        tvmin = sig._minphys * sig.gain
        tvmax = sig._maxphys * sig.gain
        if tvmin >= vmin and tvmax <= vmax:
            sig.baseline = 0
        else:
            sig.baseline = round((vmax - tvmax + vmin - tvmin) / 2)

    layout_header = os.path.join(recdir, layout_name + '.hea')
    layout.write(layout_header, fsync = fsync)

    with open(record_header, 'wt', encoding = 'UTF-8') as hf:
        if min_version:
            hf.write('#wfdb %s\n' % version_to_str(min_version))
        hf.write('%s/%d' % (recname, len(segments)))
        hf.write(' %d' % len(layout.signals))
        hf.write(' %.16g' % ffreq)
        if cfreq != ffreq or basecount != 0:
            hf.write('/%.16g' % cfreq)
            if basecount != 0:
                hf.write('(%.16g)' % basecount)
        hf.write(' %d\n' % end)
        for (name, length) in segments:
            hf.write('%s %d\n' % (name, length))

        if fsync:
            hf.flush()
            fdatasync(hf.fileno())
