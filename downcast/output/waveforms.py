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

from ..messages import WaveSampleMessage

class WaveSampleHandler:
    def __init__(self, archive):
        self.archive = archive
        self.files = set()
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

        # Look up the corresponding record and add event to the time map
        record = self.archive.get_record(msg, (ttl <= 0))
        if record is None:
            # Record not yet available - hold message in pending and
            # continue processing
            return

        info = self.info.get(record)
        if info is None:
            info = self.info[record] = WaveOutputInfo()

        # FIXME: things are likely to break interestingly with
        # non-power-of-two tps; we should make an effort to not
        # completely fall apart in that case.

        # Also tps > tpf for low resolution signals like Resp

        # Also, if the wave ID doesn't exist / doesn't have a valid
        # sample_period, should set tps to something reasonable.

        tps = attr.sample_period
        nsamples = len(msg.wave_samples) // 2

        # FIXME: is sequence_number the start or end?
        msg_start = msg.sequence_number - record.seqnum0()
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
        for f in self.files:
            f.flush()
        self.files = set()
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

class WaveOutputInfo:
    def __init__(self):
        self.signal_buffer = SignalBuffer()
        self.last_seen_time = None
        self.signal_file = None
        self.frame_offset = {}
        self.frame_size = None

        # FIXME: output state must be saved/restored (using properties
        # and/or header file)
        self.segment_signals = []
        self.segment_start = None
        self.segment_end = None
        self.flushed_time = None

    def close_segment(self, record):
        if self.signal_file is not None:
            record.close_file(self.signal_file)
        self.signal_file = None
        self.segment_signals = []
        self.segment_start = None
        self.frame_offset = {}
        self.frame_size = None

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
                spf = -(-_tpf // signal.sample_period) # XXX
                gain = 200     # XXX
                baseline = 0   # XXX
                units = signal.unit_label # XXX
                adcres = 16    # XXX
                adczero = 0
                desc = signal.label # XXX
                hf.write('%s %dx%d %g(%d)/%s %d %d 0 0 0 %s\n'
                         % (datname, _fmt, spf, gain, baseline,
                            units, adcres, adczero, desc))
                self.frame_offset[signal] = self.frame_size
                self.frame_size += spf

        self.signal_file = datname
        self.segment_signals = signals
        self.segment_start = start
        self.segment_end = start

    def write_signals(self, record, start, end, sigdata):
        # FIXME: we don't want to sort by WaveAttr specifically
        signals = sorted(sigdata)

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

