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
import heapq
import logging
from decimal import Decimal

from ..messages import WaveSampleMessage
from ..attributes import WaveAttr

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
        record = self.archive.get_record(msg, (ttl <= 0))
        if record is None:
            # Record not yet available - hold message in pending and
            # continue processing
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
            msg_start = 0
        else:
            msg_start = msg.sequence_number - s0
        msg_start -= msg_start % tps
        msg_end = msg_start + nsamples * tps

        # If we have already flushed past the end of this message,
        # nothing to do
        if info.flushed_time is not None and msg_end < info.flushed_time:
            source.ack_message(chn, msg, self)
            return

        # Add signal data to the buffer
        for (vstart, vend) in _valid_sample_intervals(msg):
            t0 = msg_start + vstart * tps
            t1 = msg_start + vend * tps
            s = msg.wave_samples[2*vstart:2*vend]
            info.signal_buffer.add_signal(attr, tps, t0, s)

        if info.last_seen_time is None or msg_start > info.last_seen_time:
            info.last_seen_time = msg_start

        # Determine how far we can flush up to (assuming all waves
        # prior to flush_time have now been recorded in the buffer)
        if ttl <= 0:
            flush_time = msg_end
        else:
            flush_time = info.last_seen_time

        # FIXME: when finalizing the record, want to flush all
        # remaining data

        # Write out buffered data up to flush_time
        updated = False
        while (info.flushed_time is None or info.flushed_time < flush_time):
            if info.flushed_time is not None:
                info.signal_buffer.truncate_before(info.flushed_time)

            (start, end, sigdata) = info.signal_buffer.get_signals()
            if sigdata is None or start >= flush_time:
                break
            if end > flush_time:
                end = flush_time
            if (info.flushed_time is not None and end <= info.flushed_time):
                break
            info.write_signals(record, start, end, sigdata)
            info.flushed_time = end
            updated = True

        # If the entire message has now been written, then acknowledge it
        if (info.flushed_time is not None and info.flushed_time >= msg_end):
            source.ack_message(chn, msg, self)
        # otherwise, check if we are now able to acknowledge older messages
        elif updated:
            source.nack_message(chn, msg, self, replay = True)

    def flush(self):
        for (record, info) in self.info.items():
            info.flush_signals(record)
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

def _valid_sample_intervals(msg):
    """Get a list of valid sample intervals in a WaveSampleMessage."""

    # This excludes all samples that are either 'invalid' or 'unavailable'.

    # XXX Determine what the difference is.  Also consider excluding,
    # e.g., zeroes (if they are out of range.)

    isl = _parse_interval_list(msg.invalid_samples)
    usl = _parse_interval_list(msg.unavailable_samples)
    cur = 0
    nsamples = len(msg.wave_samples) // 2
    for (start, end) in sorted(isl + usl):
        if start <= end and start <= nsamples:
            if start > cur:
                yield (cur, start)
            cur = end + 1
    if nsamples > cur:
        yield (cur, nsamples)

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
        else:
            s += '_'
    return s

def _get_signal_units_desc(attr):
    units = desc = None
    if attr.unit_label == '':
        units = 'NU'
    elif attr.unit_label is not None:
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

class WaveOutputInfo:
    def __init__(self, record):
        # Pending message data - not saved to disk
        self.signal_buffer = SignalBuffer()
        self.last_seen_time = None

        # Persistent state

        # flushed_time: time at which all signal data has been written
        self.flushed_time = record.get_int_property(['waves', 'flushed_time'])

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
        self.frame_size = 0
        try:
            siginfo = record.get_property(['waves', 'signals'])
            for s in siginfo:
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
        self.signal_file = None
        self.segment_signals = []
        self.segment_start = None
        self.frame_offset = {}
        self.frame_size = None
        record.set_property(['waves', 'signals'], [])
        record.set_property(['waves', 'signal_file'], None)
        record.set_property(['waves', 'segment_start'], None)
        record.set_property(['waves', 'segment_end'], None)

    def open_segment(self, record, segname, start, signals):
        self.close_segment(record)

        heaname = segname + '.hea'
        datname = segname + '.dat'

        # FIXME: use ArchiveRecord...
        heapath = os.path.join(record.path, heaname)
        self.frame_size = 0
        with open(heapath, 'wt', encoding = 'UTF-8') as hf:
            hf.write('%s %d %g/1000(%d)\n'
                     % (segname, len(signals), _ffreq, start))
            for signal in signals:
                (units, desc) = _get_signal_units_desc(signal)

                spf = -(-_tpf // signal.sample_period) # XXX

                csl = signal.calibration_scaled_lower
                csu = signal.calibration_scaled_upper
                cal = signal.calibration_abs_lower
                cau = signal.calibration_abs_upper
                if (csl != csu and cal != cau and csl and csu and cal and cau):
                    gain = (csu - csl) / (cau - cal)
                    baseline = csl - cal * gain
                else:
                    gain = 1
                    baseline = 0

                sl = signal.scale_lower
                su = signal.scale_upper
                if sl and su:
                    d = su - sl
                    adcres = 0
                    while d > 0:
                        d = d // 2
                        adcres += 1
                    adczero = (su + sl) // 2
                else:
                    adcres = adczero = 0

                hf.write('%s %dx%d %g(%d)/%s %d %d 0 0 0 %s\n'
                         % (datname, _fmt, spf, gain, baseline,
                            units, adcres, adczero, desc))

                self.frame_offset[signal] = self.frame_size
                self.frame_size += spf

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
        record.set_property(['waves', 'segment_start'], start)
        record.set_property(['waves', 'segment_end'], start)
        self.signal_file = datname
        self.segment_signals = signals
        self.segment_start = start
        self.segment_end = start

    def write_signals(self, record, start, end, sigdata):
        signals = sorted(sigdata, key = lambda a: (a.channel,
                                                   a.base_physio_id,
                                                   a.physio_id, a))

        if (signals != self.segment_signals
                or self.segment_end is None
                or start > self.segment_end
                or start < self.segment_start):
            self.open_segment(record, ('%09d' % start), start, signals)

        sf = record.open_bin_file(self.signal_file)

        # FIXME: this could be waaaay optimized, and should be

        for (signal, samples) in sigdata.items():
            spf = -(-_tpf // signal.sample_period)
            t0 = (start - self.segment_start) // signal.sample_period
            n = (end - start) // signal.sample_period
            for i in range(0, n):
                fn = (t0 + i) // spf
                sn = (t0 + i) % spf
                ind = fn * self.frame_size + self.frame_offset[signal] + sn
                sf.write(ind * 2, samples[2*i:2*i+2])

        if end > self.segment_end:
            self.segment_end = end

    def flush_signals(self, record):
        if self.signal_file is not None:
            sf = record.open_bin_file(self.signal_file)
            sf.flush()
        record.set_property(['waves', 'segment_start'], self.segment_start)
        record.set_property(['waves', 'segment_end'], self.segment_end)
        record.set_property(['waves', 'flushed_time'], self.flushed_time)

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

