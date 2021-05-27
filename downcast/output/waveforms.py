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

import array
import os
import heapq
import logging
import re
from decimal import Decimal

from ..messages import WaveSampleMessage
from ..attributes import WaveAttr
from .wfdb import (AnnotationType, Annotator, join_segments,
                   SegmentHeader, SignalInfo)

class WaveSampleHandler:
    def __init__(self, archive):
        self.archive = archive
        self.info = {}

    def send_message(self, chn, msg, source, ttl):
        if not isinstance(msg, WaveSampleMessage):
            return

        source.nack_message(chn, msg, self)

        # Load metadata for this waveform
        attr = msg.origin.get_wave_attr(msg.wave_id, (ttl <= 0))
        if attr is None:
            # Metadata not yet available - hold message in pending and
            # continue processing
            return

        # Look up the corresponding record
        record = self.archive.get_record(msg)
        if record is None:
            # Record not yet available - hold message in pending and
            # continue processing
            return

        # Dump original message to BCP file if desired
        if record.dump(msg):
            source.ack_message(chn, msg, self)
            return

        # Add event to the time map
        record.set_time(msg.sequence_number, msg.timestamp)

        info = self.info.get(record)
        if info is None:
            info = self.info[record] = WaveOutputInfo(record)

        # FIXME: things are likely to break interestingly with
        # non-power-of-two tps; we should make an effort to not
        # completely fall apart in that case.

        # Also tps > tpf for low resolution signals like Resp

        # Also, if the wave ID doesn't exist / doesn't have a valid
        # sample_period, should set tps to something reasonable.

        tps = attr.sample_period
        nsamples = len(msg.wave_samples) // 2

        # Determine the relative sequence number
        s0 = record.seqnum0()
        if s0 is None:
            record.set_seqnum0(msg.sequence_number)
            # XXX This should maybe be done more cleanly...
            record.flush(self.archive.deterministic_output)
            msg_start = 0
        else:
            msg_start = msg.sequence_number - s0
        msg_start -= msg_start % tps
        msg_end = msg_start + nsamples * tps

        # If we have already saved this time interval, nothing to do
        if info.interval_is_saved(msg_start, msg_end):
            self._write_events(record, msg, attr, msg_start, tps)
            source.ack_message(chn, msg, self)
            return

        if info.pending_start_time is None:
            info.pending_start_time = info.pending_end_time = msg_start

        updated = False

        if info.pending_start_time <= msg_start <= info.pending_end_time:
            # If this message overlaps with the existing buffered
            # data, then assume we have now seen all data between
            # pending_start_time and msg_start.
            if info.pending_start_time < msg_start:
                info.write_pending_signals(record, msg_start)
                updated = True
        else:
            # If this message is later than pending_end_time, then we
            # have a gap.  If this message is earlier than
            # pending_start_time (but this message itself has not yet
            # been saved), then we have a clock inconsistency.  In
            # either case, assume that we have now seen all data
            # between pending_start_time and pending_end_time.
            if msg_start < info.pending_start_time:
                logging.warning('waveforms out of order at %s (%s) in %s'
                                % (msg_start, msg.timestamp,
                                   record.record_id))
            if info.pending_start_time < info.pending_end_time:
                info.write_pending_signals(record, info.pending_end_time)
                updated = True
            info.pending_start_time = info.pending_end_time = msg_start

        # Add signal data to the buffer.
        for (t0, t1) in info.unsaved_intervals(msg_start, msg_end):
            vstart = (t0 - msg_start) // tps
            vend = (t1 - msg_start) // tps
            s = msg.wave_samples[2*vstart:2*vend]
            info.signal_buffer.add_signal(attr, tps, t0, s)
            info.pending_end_time = max(t1, info.pending_end_time)

        # If message is expiring, need to save all data from this
        # message NOW.
        if ttl <= 0:
            info.write_pending_signals(record, msg_end)
            self._write_events(record, msg, attr, msg_start, tps)
            source.ack_message(chn, msg, self)
        # otherwise, check if we are now able to acknowledge older messages
        elif updated:
            source.nack_message(chn, msg, self, replay = True)

    def _write_events(self, record, msg, attr, msg_start, tps):
        if (msg.paced_pulses
                or msg.invalid_samples
                or msg.unavailable_samples):
            # FIXME: avoid using desc in filename
            (_, desc) = _get_signal_units_desc(attr)
            logfile = record.open_log_file('_wq_%s' % desc)
            for pp in _parse_sample_list(msg.paced_pulses):
                logfile.append('P%s' % (msg_start + pp * tps))
            for (is0, is1) in _parse_interval_list(msg.invalid_samples):
                logfile.append('I%s-%s' % (msg_start + is0 * tps,
                                           msg_start + (is1 + 1) * tps))
            for (us0, us1) in _parse_interval_list(msg.unavailable_samples):
                logfile.append('U%s-%s' % (msg_start + us0 * tps,
                                           msg_start + (us1 + 1) * tps))

    def flush(self):
        self.archive.flush()

def _parse_sample_list(text):
    """Parse an ASCII string into a list of integers."""
    if text is None:
        return []
    l = []
    try:
        for i in text.split():
            l.append(int(i))
    except ValueError:
        # syntax error - ignore following text if any
        pass
    return l

def _parse_interval_list(text):
    """Parse an ASCII string into a list of pairs of integers."""
    l = _parse_sample_list(text)
    return list(zip(l[0::2], l[1::2]))

################################################################

_ffreq = 62.5
_tpf = int(round(1000 / _ffreq))
_fmt = 16

def _sanitize_desc(desc):
    s = ''
    for c in desc:
        if ord(c) >= 32 and ord(c) < 127:
            s += c
        elif c == '₂':
            s += '2'
        elif c == 'Δ':
            s += 'Delta'
        else:
            s += '_'
    return s

def _sanitize_units(units):
    s = ''
    for c in units:
        if ord(c) > 32 and ord(c) < 127:
            s += c
        elif c == '°':
            s += 'deg'
        elif c == 'µ':
            s += 'u'
        else:
            s += '_'
    return s

def _get_signal_units_desc(attr):
    units = desc = None
    if attr.unit_label == '':
        units = 'NU'
    elif attr.unit_label not in (None, 'Unknwn'):
        units = _sanitize_units(attr.unit_label)
    if attr.label is not None and attr.label != '':
        desc = _sanitize_desc(attr.label)
    if attr.base_physio_id == 131328:
        units = (units or 'mV')
        desc = (desc or ('ECG #%d' % attr.physio_id))
    elif attr.base_physio_id == 150016:
        units = (units or 'mmHg')
        desc = (desc or ('Pressure #%d' % attr.physio_id))
    elif attr.base_physio_id == 150452:
        units = (units or 'NU')
        desc = (desc or ('Pleth #%d' % attr.physio_id))
    else:
        units = (units or 'unknown')
        desc = (desc or '#%d/%d' % (attr.base_physio_id, attr.physio_id))
    return (units, desc)

# Convert string to Decimal
def _todec(val):
    if val is None:
        return None
    else:
        return Decimal(val)

# Convert Decimal to string
def _fromdec(val):
    if val is None:
        return None
    else:
        return str(val)

# Convert little-endian byte array to array of integers
if array.array('h', [0x1234]).tobytes() == b'\x34\x12':
    def _bytestoint16(val):
        arr = array.array('h')
        arr.frombytes(val)
        return arr
elif array.array('h', [0x1234]).tobytes() == b'\x12\x34':
    def _bytestoint16(val):
        arr = array.array('h')
        arr.frombytes(val)
        arr.byteswap()
        return arr

class WaveOutputInfo:
    def __init__(self, record):
        # Pending message data - not saved to disk
        self.signal_buffer = SignalBuffer()
        self.pending_start_time = None
        self.pending_end_time = None

        # Persistent state

        # saved_intervals: list of intervals for which signal data
        # has been written
        self.saved_intervals = []
        try:
            intervals = record.get_property(['waves', 'saved_intervals'])
            for (start, end) in intervals:
                self.saved_intervals.append([int(start), int(end)])
        except (KeyError, TypeError):
            pass
        self.saved_intervals.sort()

        # segment_name: name of the currently open segment, if any
        self.segment_name = record.get_str_property(['waves', 'segment_name'])

        # signal_file: name of the currently open signal file, if any
        self.signal_file = record.get_str_property(['waves', 'signal_file'])

        # segment_start: starting time (relative seqnum) of the
        # beginning of the current segment
        self.segment_start = record.get_int_property(
            ['waves', 'segment_start'])

        # segment_end: ending time (relative seqnum) of the end of the
        # current segment
        self.segment_end = record.get_int_property(['waves', 'segment_end'])

        # signals: list of signal attributes
        self.segment_signals = []
        self.frame_offset = {}
        self.sample_min = {}
        self.sample_max = {}
        self.sample_sum = {}
        self.frame_size = 0
        try:
            siginfo = record.get_property(['waves', 'signals'])
            for (snum, s) in enumerate(siginfo):
                attr = WaveAttr(
                    base_physio_id        = s['base_physio_id'],
                    physio_id             = s['physio_id'],
                    label                 = s['label'],
                    channel               = s['channel'],
                    sample_period         = s['sample_period'],
                    is_slow_wave          = s['is_slow_wave'],
                    is_derived            = s['is_derived'],
                    color                 = s['color'],
                    low_edge_frequency    = _todec(s['low_edge_frequency']),
                    high_edge_frequency   = _todec(s['high_edge_frequency']),
                    scale_lower           = s['scale_lower'],
                    scale_upper           = s['scale_upper'],
                    calibration_scaled_lower = s['calibration_scaled_lower'],
                    calibration_scaled_upper = s['calibration_scaled_upper'],
                    calibration_abs_lower = _todec(s['calibration_abs_lower']),
                    calibration_abs_upper = _todec(s['calibration_abs_upper']),
                    calibration_type      = s['calibration_type'],
                    unit_label            = s['unit_label'],
                    unit_code             = s['unit_code'],
                    ecg_lead_placement    = s['ecg_lead_placement']
                )
                self.segment_signals.append(attr)

                self.sample_min[attr] = record.get_int_property(
                    ['waves', 'sample_min', str(snum)], 32767)
                self.sample_max[attr] = record.get_int_property(
                    ['waves', 'sample_max', str(snum)], -32768)
                self.sample_sum[attr] = record.get_int_property(
                    ['waves', 'sample_sum', str(snum)], 0)

                # FIXME: maybe spf should be stored in properties explicitly
                spf = -(-_tpf // attr.sample_period)
                self.frame_offset[attr] = self.frame_size
                self.frame_size += spf

        except (KeyError, TypeError, ValueError, ArithmeticError):
            if self.signal_file is not None:
                logging.exception('unable to resume signal output')
            self.close_segment(record)

    def close_segment(self, record):
        if self.signal_file is not None:
            record.close_file(self.signal_file)
            if self.segment_name is not None:
                self._write_header(record, self.segment_name, self.signal_file,
                                   self.segment_start, self.segment_end,
                                   self.segment_signals)

        self.segment_name = None
        self.signal_file = None
        self.segment_signals = []
        self.segment_start = None
        self.frame_offset = {}
        self.sample_min = {}
        self.sample_max = {}
        self.sample_sum = {}
        self.frame_size = None
        record.set_property(['waves', 'signals'], [])
        record.set_property(['waves', 'signal_file'], None)
        record.set_property(['waves', 'segment_start'], None)
        record.set_property(['waves', 'segment_end'], None)
        record.set_property(['waves', 'sample_min'], {})
        record.set_property(['waves', 'sample_max'], {})
        record.set_property(['waves', 'sample_sum'], {})

    def interval_is_saved(self, start, end):
        for (saved_start, saved_end) in reversed(self.saved_intervals):
            if end > saved_end:
                return False
            elif start >= saved_start:
                return True
        return False

    def unsaved_intervals(self, start, end):
        intervals = []
        for (saved_start, saved_end) in reversed(self.saved_intervals):
            if start >= saved_end:
                break
            if end > saved_end:
                intervals.append([saved_end, end])
            if end > saved_start:
                end = saved_start
                if end <= start:
                    break
        if start < end:
            intervals.append([start, end])
        return intervals

    def mark_saved(self, record, start, end):
        if (self.saved_intervals
                and start <= self.saved_intervals[-1][1]
                and start >= self.saved_intervals[-1][0]):
            # extend last interval
            if self.saved_intervals[-1][1] < end:
                self.saved_intervals[-1][1] = end
        else:
            # check and merge with other saved intervals
            before = []
            after = []
            for i in self.saved_intervals:
                if i[1] < start:
                    before.append(i)
                elif i[0] > end:
                    after.append(i)
                else:
                    start = min(start, i[0])
                    end = max(end, i[1])
            self.saved_intervals = before + [[start, end]] + after
        record.set_property(['waves', 'saved_intervals'], self.saved_intervals)

    def write_pending_signals(self, record, end):
        while self.pending_start_time < end:
            (chunkstart, chunkend, sigdata) = self.signal_buffer.get_signals()
            if sigdata is None or chunkstart >= end:
                break
            if chunkend > end:
                chunkend = end
            self.write_signals(record, chunkstart, chunkend, sigdata)
            self.mark_saved(record, self.pending_start_time, chunkend)
            self.signal_buffer.truncate_before(chunkend)
            self.pending_start_time = chunkend

    def _write_header(self, record, segname, datname, start, end, signals):
        # FIXME: use ArchiveRecord...
        heaname = segname + '.hea'
        heapath = os.path.join(record.path, heaname)

        header = SegmentHeader()
        header.ffreq = _ffreq
        header.cfreq = 1000
        header.basecount = start
        if end is not None:
            header.nframes = (end - start) // _tpf

        sigdesc = []
        for (i, signal) in enumerate(signals):
            (units, desc) = _get_signal_units_desc(signal)
            while desc in sigdesc:
                desc += '+'
            sigdesc.append(desc)

            spf = -(-_tpf // signal.sample_period) # XXX

            csl = signal.calibration_scaled_lower
            csu = signal.calibration_scaled_upper
            cal = signal.calibration_abs_lower
            cau = signal.calibration_abs_upper
            try:
                gain = (csu - csl) / (cau - cal)
                if units == 'mV':
                    # ECG signals appear to be consistently
                    # mislabeled in this way (cal is 0.0 and cau
                    # is 1.0, but the baseline is roughly halfway
                    # between csl and csu... which is also roughly
                    # halfway between sl and su.  Not sure what
                    # the correct interpretation is.)
                    baseline = (csl + csu) / 2
                else:
                    baseline = csl - cal * gain
            except (TypeError, ArithmeticError):
                gain = 0
                baseline = 0

            # scale_lower/scale_upper don't seem to represent the
            # true range of the signal.  Use the actual observed
            # minimum and maximum values instead.
            sl = self.sample_min[signal]
            su = self.sample_max[signal]
            if sl is None or su is None or sl > su:
                adcres = adczero = 0
            else:
                adcres = 1
                if sl < 0:
                    adcmin = -1
                    adcmax = 1
                else:
                    adcmin = 0
                    adcmax = 2
                while su >= adcmax or sl < adcmin:
                    adcmin *= 2
                    adcmax *= 2
                    adcres += 1
                adczero = (adcmin + adcmax) // 2

            if gain == 0:
                gain = (1 << adcres)

            cksum = self.sample_sum[signal]

            siginfo = SignalInfo(fname = datname, fmt = _fmt, spf = spf,
                                 gain = gain, baseline = baseline,
                                 adcres = adcres, adczero = adczero,
                                 desc = desc, units = units, cksum = cksum)
            header.signals.append(siginfo)

            # Write additional attributes as info strings.
            info = []

            # Report channel number for ECGs.
            if signal.base_physio_id == 131328:
                info.append('channel=%d' % signal.channel)

            # Report if signal is derived (e.g. standard limb
            # leads derived from EASI leads.)
            if signal.is_derived:
                info.append('derived')

            # Report filter cutoff frequencies if known.
            if signal.low_edge_frequency and signal.high_edge_frequency:
                info.append('bandpass=[%g,%g]'
                            % (signal.low_edge_frequency,
                               signal.high_edge_frequency))
            elif signal.low_edge_frequency:
                info.append('highpass=%g' % signal.low_edge_frequency)
            elif signal.high_edge_frequency:
                info.append('lowpass=%g' % signal.high_edge_frequency)
            if info:
                infostr = ' signal %d (%s): ' % (i, sigdesc[i])
                infostr += ' '.join(info)
                header.info.append(infostr)

        header.write(heapath)

    def open_segment(self, record, segname, start, signals):
        self.close_segment(record)

        datname = segname + '.dat'

        self.frame_size = 0
        self.sample_min = {}
        self.sample_max = {}
        self.sample_sum = {}
        for signal in signals:
            spf = -(-_tpf // signal.sample_period) # XXX
            self.frame_offset[signal] = self.frame_size
            self.sample_min[signal] = 32767
            self.sample_max[signal] = -32768
            self.sample_sum[signal] = 0
            self.frame_size += spf

        self._write_header(record, segname, datname, start, None, signals)

        sigprop = []
        for attr in signals:
            sigprop.append({
                'base_physio_id':           attr.base_physio_id,
                'physio_id':                attr.physio_id,
                'label':                    attr.label,
                'channel':                  attr.channel,
                'sample_period':            attr.sample_period,
                'is_slow_wave':             attr.is_slow_wave,
                'is_derived':               attr.is_derived,
                'color':                    attr.color,
                'low_edge_frequency':  _fromdec(attr.low_edge_frequency),
                'high_edge_frequency': _fromdec(attr.high_edge_frequency),
                'scale_lower':              attr.scale_lower,
                'scale_upper':              attr.scale_upper,
                'calibration_scaled_lower': attr.calibration_scaled_lower,
                'calibration_scaled_upper': attr.calibration_scaled_upper,
                'calibration_abs_lower': _fromdec(attr.calibration_abs_lower),
                'calibration_abs_upper': _fromdec(attr.calibration_abs_upper),
                'calibration_type':         attr.calibration_type,
                'unit_label':               attr.unit_label,
                'unit_code':                attr.unit_code,
                'ecg_lead_placement':       attr.ecg_lead_placement
            })
        record.set_property(['waves', 'signals'], sigprop)
        record.set_property(['waves', 'signal_file'], datname)
        record.set_property(['waves', 'segment_name'], segname)
        record.set_property(['waves', 'segment_start'], start)
        record.set_property(['waves', 'segment_end'], start)
        self.segment_name = segname
        self.signal_file = datname
        self.segment_signals = signals
        self.segment_start = start
        self.segment_end = start

    def write_signals(self, record, start, end, sigdata):
        signals = sorted(sigdata, key = lambda a: (a.base_physio_id,
                                                   a.channel,
                                                   a.physio_id, a))

        if (signals != self.segment_signals
                or self.segment_end is None
                or start > self.segment_end
                or start < self.segment_start):
            self.open_segment(record, ('%010d' % start), start, signals)

        sf = record.open_bin_file(self.signal_file)

        if start < self.segment_end:
            # this shouldn't happen
            logging.warning('skipping already-written data')
            if end < self.segment_end:
                return
            nsigdata = {}
            for (signal, samples) in sigdata.items():
                skip = (self.segment_end - start) // signal.sample_period
                nsigdata[signal] = samples[2*skip:]
            start = self.segment_end
            sigdata = nsigdata

        # FIXME: this could be waaaay optimized, and should be

        for (snum, signal) in enumerate(signals):
            samples = sigdata[signal]
            spf = -(-_tpf // signal.sample_period)
            t0 = (start - self.segment_start) // signal.sample_period
            n = (end - start) // signal.sample_period
            if signal.scale_lower and signal.scale_lower > 0:
                zsub = b'\0\x80'
                ssub = -32768
            else:
                zsub = b'\0\0'
                ssub = 0
            svalues = _bytestoint16(samples[0:2*n])
            smin = min(svalues)
            smax = max(svalues)
            ssum = sum(svalues)
            for i in range(0, n):
                fn = (t0 + i) // spf
                sn = (t0 + i) % spf
                ind = fn * self.frame_size + self.frame_offset[signal] + sn
                sv = samples[2*i:2*i+2]
                if sv == b'\0\0':
                    sv = zsub
                    ssum += ssub
                sf.write(ind * 2, sv)

            smin = min(self.sample_min[signal], smin)
            smax = max(self.sample_max[signal], smax)
            ssum = (ssum + self.sample_sum[signal]) & 0xffff
            self.sample_min[signal] = smin
            self.sample_max[signal] = smax
            self.sample_sum[signal] = ssum
            record.set_property(['waves', 'sample_min', str(snum)], smin)
            record.set_property(['waves', 'sample_max', str(snum)], smax)
            record.set_property(['waves', 'sample_sum', str(snum)], ssum)

        self.segment_end = end
        record.set_property(['waves', 'segment_end'], self.segment_end)

################################################################

class SignalBuffer:
    """Object that tracks signal availability over time."""
    def __init__(self):
        self.signals = {}

    def add_signal(self, signal, tps, start, samples):
        """Add signal data to the buffer."""
        if len(samples) == 0:
            return
        info = self.signals.get(signal)
        if info is None:
            self.signals[signal] = (tps, [(start, samples)])
            return
        else:
            heapq.heappush(info[1], (start, samples))

    def truncate_before(self, t):
        """Delete data preceding a given point in time."""
        dropped_signals = set()
        for (signal, (tps, smap)) in self.signals.items():
            while len(smap) > 0 and smap[0][0] <= t - tps:
                (start0, samples0) = smap[0]
                skipsamples = (t - start0) // tps
                if len(samples0) > skipsamples * 2:
                    newstart = start0 + skipsamples * tps
                    newsamples = samples0[skipsamples*2:]
                    heapq.heapreplace(smap, (newstart, newsamples))
                else:
                    heapq.heappop(smap)
            if len(smap) == 0:
                dropped_signals.add(signal)
        for signal in dropped_signals:
            del self.signals[signal]

    def get_signals(self):
        """Retrieve a homogeneous chunk from the start of the buffer."""

        # FIXME: keep a sorted list of signals, so that data can be a
        # sorted list rather than a dictionary that we need to sort
        # every time

        start = None
        end = None
        data = None
        for (signal, (tps, smap)) in self.signals.items():
            (start0, samples0) = smap[0]
            end0 = start0 + len(samples0) // 2 * tps
            if end is None:
                start = start0
                end = end0
                data = {signal: samples0}
            elif start0 < start:
                end = min(end0, start)
                start = start0
                data = {signal: samples0}
            elif start0 == start:
                end = min(end0, end)
                data[signal] = samples0
            else:
                end = min(start0, end)
        return (start, end, data)

################################################################

class WaveSampleFinalizer:
    def __init__(self, record):
        self.record = record

    def finalize_record(self):
        record = self.record
        info = WaveOutputInfo(record)
        info.close_segment(record)

        # Find all segments and construct the multi-segment header
        segments = []
        for f in os.listdir(record.path):
            if re.fullmatch(r'-?[0-9]+\.hea', f):
                n = int(f.split('.')[0])
                segments.append((n, f))
        segments.sort()
        if not segments:
            return
        headers = [os.path.join(record.path, s[1]) for s in segments]
        join_segments(os.path.join(record.path, 'waves.hea'), headers)

        # Read _wq files and write wave quality annotations
        annfname = os.path.join(self.record.path, 'waves.wq')
        with Annotator(annfname, afreq = 1000) as wqanns:
            layoutfname = os.path.join(record.path, 'waves_layout.hea')
            layout = SegmentHeader(layoutfname)
            for (chan, sig) in enumerate(layout.signals):
                try:
                    wqfname = os.path.join(record.path, '_wq_%s' % sig.desc)
                    wqfile = open(wqfname, 'rb')
                except FileNotFoundError:
                    continue

                ppat = re.compile(b'P(\d+)\n')
                ipat = re.compile(b'I(\d+)-(\d+)\n')
                upat = re.compile(b'U(\d+)-(\d+)\n')

                # events[t][0] = number of "invalid" intervals at time t
                #                minus number of intervals at time (t-1)
                # events[t][1] = number of "unavailable" intervals at time t
                #                minus number of intervals at time (t-1)
                events = {}
                for line in wqfile:
                    # Paced pulses: write a single annotation for each
                    m = ppat.fullmatch(line)
                    if m:
                        t = int(m.group(1))
                        wqanns.put(time = int(m.group(1)), chan = chan,
                                   anntyp = AnnotationType.PACESP)
                        continue

                    # Invalid and unavailable samples: update counters
                    # so we can write an annotation when the sample
                    # state goes from "invalid" to "not invalid" and
                    # vice versa.
                    m = ipat.fullmatch(line)
                    if m:
                        t0 = int(m.group(1))
                        t1 = int(m.group(2))
                        e0 = events.setdefault(t0, [0, 0])
                        e0[0] += 1
                        e1 = events.setdefault(t1, [0, 0])
                        e1[0] -= 1
                        continue
                    m = upat.fullmatch(line)
                    if m:
                        t0 = int(m.group(1))
                        t1 = int(m.group(2))
                        e0 = events.setdefault(t0, [0, 0])
                        e0[1] += 1
                        e1 = events.setdefault(t1, [0, 0])
                        e1[1] -= 1
                        continue
                wqfile.close()
                icount = ucount = 0
                for (t, e) in sorted(events.items()):
                    prev_icount = icount
                    prev_ucount = ucount
                    icount += e[0]
                    ucount += e[1]
                    if icount and not prev_icount:
                        wqanns.put(time = t, chan = chan,
                                   anntyp = AnnotationType.NOTE, aux = '(i')
                    if prev_icount and not icount:
                        wqanns.put(time = t, chan = chan,
                                   anntyp = AnnotationType.NOTE, aux = 'i)')
                    if ucount and not prev_ucount:
                        wqanns.put(time = t, chan = chan,
                                   anntyp = AnnotationType.NOTE, aux = '(u')
                    if prev_ucount and not ucount:
                        wqanns.put(time = t, chan = chan,
                                   anntyp = AnnotationType.NOTE, aux = 'u)')
