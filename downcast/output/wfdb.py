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
import enum
import math
import os
import re
import struct

from ..util import fdatasync

def str_to_version(s):
    """Split a version string into a tuple of integers."""
    return tuple(int(n) for n in s.split('.'))

def version_to_str(v):
    """Convert a tuple of integers into a version string."""
    return '.'.join(str(n) for n in v)

class AnnotationType(enum.IntEnum):
    NOTQRS   = 0  #     not-QRS (not a getann/putann code)
    NORMAL   = 1  # N   normal beat
    LBBB     = 2  # L   left bundle branch block beat
    RBBB     = 3  # R   right bundle branch block beat
    ABERR    = 4  # a   aberrated atrial premature beat
    PVC      = 5  # V   premature ventricular contraction
    FUSION   = 6  # F   fusion of ventricular and normal beat
    NPC      = 7  # J   nodal (junctional) premature beat
    APC      = 8  # A   atrial premature contraction
    SVPB     = 9  # S   premature or ectopic supraventricular beat
    VESC     = 10 # E   ventricular escape beat
    NESC     = 11 # j   nodal (junctional) escape beat
    PACE     = 12 # /   paced beat
    UNKNOWN  = 13 # Q   unclassifiable beat
    NOISE    = 14 # ~   signal quality change
    ARFCT    = 16 # |   isolated QRS-like artifact
    STCH     = 18 # s   ST change
    TCH      = 19 # T   T-wave change
    SYSTOLE  = 20 # *   systole
    DIASTOLE = 21 # D   diastole
    NOTE     = 22 # "   comment annotation
    MEASURE  = 23 # =   measurement annotation
    PWAVE    = 24 # p   P-wave peak
    BBB      = 25 # B   left or right bundle branch block
    PACESP   = 26 # ^   non-conducted pacer spike
    TWAVE    = 27 # t   T-wave peak
    RHYTHM   = 28 # +   rhythm change
    UWAVE    = 29 # u   U-wave peak
    LEARN    = 30 # ?   learning
    FLWAV    = 31 # !   ventricular flutter wave
    VFON     = 32 # [   start of ventricular flutter/fibrillation
    VFOFF    = 33 # ]   end of ventricular flutter/fibrillation
    AESC     = 34 # e   atrial escape beat
    SVESC    = 35 # n   supraventricular escape beat
    LINK     = 36 # @   link to external data (aux contains URL)
    NAPC     = 37 # x   non-conducted P-wave (blocked APB)
    PFUS     = 38 # f   fusion of paced and normal beat
    WFON     = 39 # (   waveform onset
    WFOFF    = 40 # )   waveform end
    RONT     = 41 # r   R-on-T premature ventricular contraction

_tag_SKIP = 59 << 10
_tag_NUM  = 60 << 10
_tag_SUB  = 61 << 10
_tag_CHN  = 62 << 10
_tag_AUX  = 63 << 10

_u16 = struct.Struct('<H').pack
_s32 = struct.Struct('<i').pack

def _skip(n):
    d = _s32(n)
    return _u16(_tag_SKIP) + d[2:4] + d[0:2]

class Annotator:
    """Class for writing WFDB annotation files.

    After creating an Annotator, add annotations by calling put.
    Annotations do not need to be added in order, and duplicates will
    be ignored.

    This class does not support defining custom annotation types.
    """
    def __init__(self, filename, afreq = None):
        self._filename = filename
        self._annots = set()
        self._afreq = afreq

    def __len__(self):
        return len(self._annots)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def put(self, time, anntyp, subtyp = 0, chan = 0, num = None, aux = None):
        """Add an annotation.

        time is the sample number (multiple of 1/afreq).

        anntyp is the annotation type (see AnnotationType).

        chan is the channel (signal number).

        subtyp and num are extra user-defined fields which can each
        store an integer value between -128 and 127.  If num is
        unspecified, values will be assigned automatically to avoid
        creating multiple annotations with the same tuple of (time,
        chan, num).

        aux is a user-defined field which can hold a string of up to
        255 bytes.
        """
        if anntyp < 0 or anntyp > 49:
            raise ValueError('invalid anntyp: %r' % (anntyp,))
        if subtyp < -128 or subtyp > 127:
            raise ValueError('invalid subtyp: %r' % (subtyp,))
        if chan < 0 or chan > 255:
            raise ValueError('invalid chan: %r' % (chan,))
        if num is None:
            num = 256
        elif num < -128 or num > 127:
            raise ValueError('invalid num: %r' % (num,))
        if isinstance(aux, str):
            aux = aux.encode()
        if aux is not None and len(aux) > 255:
            raise ValueError('aux string too long: %r' % (aux,))

        self._annots.add((time, num, chan, anntyp, subtyp, aux))

    def close(self, fsync = True):
        """Write annotations to the output file."""
        if not self._annots:
            return

        self._fp = open(self._filename, 'wb')
        self._time = 0
        self._num = 0
        self._chan = 0
        if self._afreq is not None:
            t = ("## time resolution: %.17g" % (self._afreq,)).encode()
            self._writeann((0, 0, 0, AnnotationType.NOTE, 0, t))
            self._writeann((0, 0, 0, AnnotationType.NOTQRS, 0, None))
        for a in sorted(self._annots):
            self._writeann(a)
        self._fp.write(b'\0\0')
        if fsync:
            self._fp.flush()
            fdatasync(self._fp.fileno())
        self._fp.close()
        self._fp = None
        self._annots = None

    def _writeann(self, annot):
        (time, num, chan, anntyp, subtyp, aux) = annot
        if num == 256:
            if time != self._time or chan != self._chan:
                num = 0
            else:
                num = self._num + 1
        delta = time - self._time
        while delta < -0x7fffffff:
            self._fp.write(_skip(-0x7fffffff))
            delta -= -0x7fffffff
        while delta > 0x7fffffff:
            self._fp.write(_skip(0x7fffffff))
            delta -= 0x7fffffff
        if anntyp == 0:
            self._fp.write(_skip(delta - 1))
            delta = 1
        elif delta < 0 or delta > 1023:
            self._fp.write(_skip(delta))
            delta = 0
        self._fp.write(_u16((anntyp << 10) + delta))
        if subtyp != 0:
            self._fp.write(_u16(_tag_SUB + (subtyp & 0xff)))
        if chan != self._chan:
            self._fp.write(_u16(_tag_CHN + (chan & 0xff)))
        if num != self._num:
            self._fp.write(_u16(_tag_NUM + (num & 0xff)))
        if aux is not None:
            self._fp.write(_u16(_tag_AUX + len(aux)))
            self._fp.write(aux)
            if len(aux) % 2:
                self._fp.write(b'\0')
        self._time = time
        self._chan = chan
        self._num = num

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

        text = []
        if self.min_version:
            text.append('#wfdb %s\n' % version_to_str(self.min_version))
        text.append('%s %d %.16g' % (recname, len(self.signals), self.ffreq))
        if self.cfreq != self.ffreq or self.basecount != 0:
            text.append('/%.16g' % (self.cfreq,))
            if self.basecount != 0:
                text.append('(%.16g)' % (self.basecount,))
        if self.nframes is not None:
            text.append(' %d' % (self.nframes,))
        text.append('\n')

        for sig in self.signals:
            text.append('%s %d' % (sig.fname, sig.fmt))
            if sig.spf != 1:
                text.append('x%d' % (sig.spf,))
            if sig.skew != 0:
                text.append(':%d' % (sig.skew,))
            if sig.start != 0:
                text.append(':%d' % (sig.start,))
            text.append(' %.16g' % (sig.gain,))
            if sig.baseline != sig.adczero:
                text.append('(%d)' % (sig.baseline,))
            if sig.units is not None:
                text.append('/%s' % (sig.units,))
            text.append(' %d %d %d %d %d %s\n'
                     % (sig.adcres, sig.adczero, sig.initval,
                        sig.cksum, sig.bsize, sig.desc))
        for info in self.info:
            text.append('#%s\n' % (info,))

        text = ''.join(text)
        with open(path, 'wt', encoding = 'UTF-8') as hf:
            hf.write(text)
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
    basecount = None
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
            basecount = min(0, seg.basecount)
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
        if sig.adcres > 0:
            vmin = sig.adczero - (1 << (sig.adcres - 1))
            vmax = vmin + (1 << sig.adcres) - 1
        else:
            vmin = vmax = sig.adczero

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
