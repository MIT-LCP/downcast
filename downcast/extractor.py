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

from collections import OrderedDict
from datetime import timedelta
import json
import os
import hashlib
import logging
import sys
import time

from .dispatcher import Dispatcher
from .parser import (WaveSampleParser, NumericValueParser,
                     EnumerationValueParser, AlertParser,
                     PatientMappingParser, PatientBasicInfoParser,
                     PatientDateAttributeParser,
                     PatientStringAttributeParser, BedTagParser)
from .timestamp import (T, very_old_timestamp)

class Extractor:
    def __init__(self, db, dest_dir, fatal_exceptions = False, debug = False):
        self.db = db
        self.dest_dir = dest_dir
        self.queues = []
        self.dispatcher = Dispatcher(fatal_exceptions = fatal_exceptions)
        self.conn = db.connect()
        self.current_timestamp = very_old_timestamp
        self.queue_timestamp = OrderedDict()
        if dest_dir is not None:
            os.makedirs(dest_dir, exist_ok = True)
        self.dispatcher.add_dead_letter_handler(DefaultDeadLetterHandler())
        self.debug = debug

    def add_queue(self, queue):
        """Add an input queue."""
        self.queues.append(queue)
        self.queue_timestamp[queue] = very_old_timestamp
        if self.dest_dir is not None:
            queue.load_state(self.dest_dir)

    def add_handler(self, handler):
        """Add a message handler."""
        self.dispatcher.add_handler(handler)

    def flush(self):
        """Flush all output handlers, and save queue state to disk."""
        self.dispatcher.flush()
        if self.dest_dir is not None:
            for queue in self.queues:
                queue.save_state(self.dest_dir)

    def idle(self):
        """Check whether all available messages have been received.

        This means that run() will not do any further processing until
        new messages are added to the input database.
        """

        # Find the most out-of-date queue.
        q = min(self.queues, key = self.queue_timestamp.get)

        # If the oldest queue timestamp is greater than the current
        # time, then all queues must now be idle.
        if self.queue_timestamp[q] > self.current_timestamp:
            return True

        # Check if this queue is stalled waiting for another queue.
        sq = q.stalling_queue()
        while sq is not None:
            q = sq
            sq = q.stalling_queue()

        # Check whether that queue is idle.
        return (self.queue_timestamp[q] > self.current_timestamp)

    def run(self):
        """Perform some amount of work.

        This will execute a small number of queries (usually only
        one), reading a batch of messages from the most out-of-date
        queue and sending those messages to the attached handlers.
        """

        # Find the most out-of-date queue.
        q = min(self.queues, key = self.queue_timestamp.get)

        # If the oldest queue timestamp is greater than the current
        # timestamp, then *all* queues must now be idle; in that case,
        # ignore timestamps and handle queues in round-robin order.
        if self.queue_timestamp[q] > self.current_timestamp:
            q = next(iter(self.queue_timestamp))
            self.queue_timestamp.move_to_end(q)

        # Retrieve and submit a batch of messages.
        try:
            cursor = self.conn.cursor()

            # Check if this queue is stalled (waiting for another
            # queue before it can proceed.)  In that case, the other
            # queue inherits this one's priority.
            origq = q
            sq = q.stalling_queue()
            while sq is not None:
                q = sq
                sq = q.stalling_queue()

            # If the original queue was stalled, and the current queue
            # is up-to-date, then check all queues to update the current
            # time.  This avoids looping indefinitely if the messages
            # we're anticipating never actually show up.
            if q is not origq and q.reached_present():
                self._update_current_time(cursor)

            self._run_queries(q, cursor)
        finally:
            cursor.close()

    def _run_queries(self, queue, cursor):
        parser = queue.next_message_parser(self.db)

        if self.debug:
            dbg_start = getattr(queue, 'newest_seen_timestamp', None)
            dbg_duration = getattr(queue, 'last_batch_duration', None)
            if dbg_duration:
                dbg_duration = dbg_duration.total_seconds()
            dbg_clock_start = time.monotonic()
            sys.stderr.write('%s %s+%s'
                             % (type(queue).__name__,
                                dbg_start, dbg_duration))
            j = 0

        for msg in self.db.get_messages(parser, cursor = cursor):
            if self.debug:
                if j > 0:
                    j -= 1
                else:
                    sys.stderr.write('.')
                    sys.stderr.flush()
                    j = 16

            ts = queue.message_timestamp(msg)

            # FIXME: should disregard timestamps that are
            # completely absurd (but maybe those should be
            # thrown away at a lower level.)

            # current_timestamp = maximum timestamp of any
            # message we've seen so far
            if ts > self.current_timestamp:
                self.current_timestamp = ts

            # query_time = maximum timestamp of any
            # message we've seen in this queue
            if ts > queue.query_time:
                queue.query_time = ts

            queue.push_message(msg, self.dispatcher)

        if self.debug:
            dbg_clock_elapsed = time.monotonic() - dbg_clock_start
            dbg_newest = getattr(queue, 'last_batch_count_at_newest', None)
            dbg_total = getattr(queue, 'last_batch_count', None)
            dbg_limit = getattr(queue, 'last_batch_limit', None)
            dbg_end = getattr(queue, 'newest_seen_timestamp', None)
            if dbg_start and dbg_end:
                dbg_advance = (dbg_end - dbg_start).total_seconds()
                dbg_rate = dbg_advance / dbg_clock_elapsed
            else:
                dbg_advance = 0
                dbg_rate = 0
            sys.stderr.write('(%s/%s/%s) +%s %.2gx'
                             % (dbg_newest, dbg_total, dbg_limit,
                                dbg_advance, dbg_rate))

        # If this queue has reached the present time, put it to
        # sleep for some minimum time period before hitting it
        # again.  The delay time is dependent on the queue type.
        if queue.reached_present():
            if self.debug:
                sys.stderr.write('(done)\n')
            queue.query_time = self.current_timestamp
            self.queue_timestamp[queue] = (self.current_timestamp
                                           + queue.idle_delay())
        else:
            if self.debug:
                sys.stderr.write('\n')
            self.queue_timestamp[queue] = (queue.query_time + queue.bias())

    def _update_current_time(self, cursor):
        for queue in self.queues:
            parser = queue.final_message_parser(self.db)
            for msg in self.db.get_messages(parser, cursor = cursor):
                ts = queue.message_timestamp(msg)
                if ts > self.current_timestamp:
                    self.current_timestamp = ts

class ExtractorQueue:
    def __init__(self, queue_name, start_time = None, end_time = None,
                 messages_per_batch = 10000):
        self.queue_name = queue_name
        self.newest_seen_timestamp = start_time
        self.oldest_unacked_timestamp = start_time
        self.end_time = end_time
        self.acked_saved = {}
        self.acked_new = OrderedDict()
        self.unacked_new = OrderedDict()
        self.limit_per_batch = messages_per_batch
        self.last_batch_count_at_newest = 0
        self.last_batch_limit = 0
        self.last_batch_count = 0
        self.last_batch_end = None
        self.last_batch_duration = None
        self.query_time = very_old_timestamp

    def load_state(self, dest_dir):
        filename = self._state_file_name(dest_dir)
        try:
            with open(filename, 'rt', encoding = 'UTF-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            return
        try:
            ts = T(data['time'])
            self.newest_seen_timestamp = ts
            self.oldest_unacked_timestamp = ts
        except KeyError:
            return
        self.acked_saved = {}
        if data['acked']:
            for (tsstr, msgstrs) in data['acked'].items():
                ts = T(tsstr)
                for msgstr in msgstrs:
                    if ts not in self.acked_saved:
                        self.acked_saved[ts] = set()
                    self.acked_saved[ts].add(msgstr)

    def save_state(self, dest_dir):
        data = {}
        if self.oldest_unacked_timestamp is not None:
            data['time'] = str(self.oldest_unacked_timestamp)
            data['acked'] = {}
            for (ts, msgstrs) in self.acked_saved.items():
                tsstr = str(ts)
                for msgstr in msgstrs:
                    if tsstr not in data['acked']:
                        data['acked'][tsstr] = []
                    data['acked'][tsstr].append(msgstr)
            for (ts, cmsgs) in self.acked_new.items():
                tsstr = str(ts)
                for (chn, msg) in cmsgs:
                    if tsstr not in data['acked']:
                        data['acked'][tsstr] = []
                    data['acked'][tsstr].append(self._message_hash(msg))
        filename = self._state_file_name(dest_dir)
        tmpfname = filename + '.tmp'
        with open(tmpfname, 'wt', encoding = 'UTF-8') as f:
            json.dump(data, f)
            f.write('\n')
            f.flush()
            os.fdatasync(f.fileno())
        os.rename(tmpfname, filename)

    def _state_file_name(self, dest_dir):
        return os.path.join(dest_dir, '%' + self.queue_name + '.queue')

    def _message_hash(self, msg):
        m = hashlib.sha256()
        m.update(repr(msg).encode('UTF-8'))
        return m.hexdigest()

    def next_message_parser(self, db):
        if self.newest_seen_timestamp is None:
            # We know nothing.  Simply read the N earliest messages
            # from the table.
            n = self.limit_per_batch
            d = None

        elif (self.last_batch_count > self.last_batch_count_at_newest
              or self.last_batch_duration is None):
            # Our last query gave results from multiple timestamps (or
            # our last query was the very first, so it didn't have a
            # duration), so advance by the default batch duration.
            n = self.limit_per_batch
            d = self.default_batch_duration()

        elif self.last_batch_count < self.last_batch_limit:
            # Our last query gave results for only one timestamp, and
            # fewer than the batch limit; temporarily increase the
            # duration.
            n = self.last_batch_limit
            d = self.last_batch_duration * 2

        else:
            # Our last query gave results for only one timestamp, and
            # hit the batch limit; temporarily increase the limit.
            n = self.last_batch_limit * 2
            d = self.last_batch_duration

        start = self.newest_seen_timestamp
        if start is None:
            end = self.end_time
        else:
            if self.end_time is not None:
                d = min(d, self.end_time - start)
            end = start + d
        self.last_batch_limit = n
        self.last_batch_end = end
        self.last_batch_duration = d
        self.last_batch_count = 0
        self.last_batch_count_at_newest = 0
        return self.message_parser(db, n, time_ge = start, time_le = end)

    def final_message_parser(self, db):
        return self.message_parser(db, 1,
                                   time_ge = self.newest_seen_timestamp,
                                   time_lt = self.end_time,
                                   reverse = True)

    def reached_present(self):
        if self.end_time is None:
            # XXX This is broken.  With requirement for time-limited
            # queries we need a different approach for real-time
            # conversion.
            return (self.last_batch_count < self.last_batch_limit)
        else:
            return (self.last_batch_end >= self.end_time
                    and self.last_batch_count < self.last_batch_limit)

    def stalling_queue(self):
        return None

    def push_message(self, message, dispatcher):
        ts = self.message_timestamp(message)
        channel = self.message_channel(message)
        ttl = self.message_ttl(message)
        self.last_batch_count += 1

        if self.newest_seen_timestamp is not None:
            if ts < self.newest_seen_timestamp:
                # FIXME: in case of bad weirdness, maybe what we want
                # here is to send the message immediately, with ttl of
                # zero (and dispatcher could recognize that case
                # specifically.)
                self._log_warning('Unexpected message at %s; ignored' % ts)
                return

        if ts == self.newest_seen_timestamp:
            self.last_batch_count_at_newest += 1
        else:
            self.newest_seen_timestamp = ts
            self.last_batch_count_at_newest = 1

        if ts not in self.unacked_new:
            self.unacked_new[ts] = set()
        if ts not in self.acked_new:
            self.acked_new[ts] = set()

        # Check if this message has already been seen (acked or
        # otherwise)
        if (channel, message) in self.unacked_new[ts]:
            return
        if (channel, message) in self.acked_new[ts]:
            return

        # Check if the message was acked in a previous run.
        # Generating _message_hash(message) may be expensive so don't
        # do it if we don't have to.
        if ts in self.acked_saved:
            mstr = self._message_hash(message)
            if mstr in self.acked_saved[ts]:
                self.acked_saved[ts].discard(mstr)
                if len(self.acked_saved[ts]) == 0:
                    del self.acked_saved[ts]
                self.acked_new[ts].add((channel, message))
                return

        self.unacked_new[ts].add((channel, message))
        self._update_pointer()
        dispatcher.send_message(channel, message, self, ttl)

    def nack_message(self, channel, message, handler):
        pass

    def ack_message(self, channel, message, handler):
        ts = self.message_timestamp(message)
        if ts in self.unacked_new:
            self.unacked_new[ts].discard((channel, message))
            # else warn...
        if ts in self.acked_new:
            self.acked_new[ts].add((channel, message))
            # else warn...
        self._update_pointer()
        # FIXME: check leaks

    def _update_pointer(self):
        # Delete old empty lists of unacked messages
        ts = None
        while len(self.unacked_new) > 0:
            ts = next(iter(self.unacked_new))
            if len(self.unacked_new[ts]) == 0:
                del self.unacked_new[ts]
                ts = None
            else:
                break
        if ts is None:
            return
        if (self.oldest_unacked_timestamp is not None
                and ts <= self.oldest_unacked_timestamp):
            return

        # ts is now the oldest unacked timestamp
        self.oldest_unacked_timestamp = ts

        # Delete any older lists of acked messages
        while len(self.acked_new) > 0:
            ats = next(iter(self.acked_new))
            if ats < ts:
                del self.acked_new[ats]
            else:
                break

        # Delete any older lists of saved acked messages; warn if
        # those messages failed to reappear
        skipats = set()
        for ats in self.acked_saved:
            if ats < ts:
                n = len(self.acked_saved[ats])
                if n > 0:
                    self._log_warning(('Missed %d expected messages at %s; ' +
                                       'corrupt DB or window underrun?')
                                      % (n, ats))
                skipats.add(ats)
        for ats in skipats:
            del self.acked_saved[ats]

    def _log_warning(self, text):
        logging.warning(text)

class DefaultDeadLetterHandler:
    def send_message(self, channel, message, dispatcher, ttl):
        logging.warning('Unhandled message: %r' % (message,))

################################################################

class MappingIDExtractorQueue(ExtractorQueue):
    def __init__(self, queue_name, mapping_id = None,
                 patient_mapping_queue = None,
                 patient_mapping_delay = None,
                 **kwargs):
        ExtractorQueue.__init__(self, queue_name, **kwargs)
        self.mapping_id = mapping_id
        self.patient_mapping_queue = patient_mapping_queue
        self.patient_mapping_delay = patient_mapping_delay
        self.stalled_ids = {}
        self.unstalled_ids = set()

    def message_channel(self, message):
        return ('M', message.mapping_id)
    def message_timestamp(self, message):
        return message.timestamp
    def message_ttl(self, message):
        return self.limit_per_batch * 20

    def default_batch_duration(self):
        return timedelta(seconds = 11)
    def bias(self):
        return timedelta(0)

    def nack_message(self, channel, message, handler):
        ExtractorQueue.nack_message(self, channel, message, handler)
        if self.patient_mapping_queue is not None:
            db = message.origin
            mid = message.mapping_id
            ts = message.timestamp
            if ((db, mid) not in self.unstalled_ids
                    and ((db, mid) not in self.stalled_ids
                         or self.stalled_ids[db, mid] > ts)):
                self.stalled_ids[db, mid] = ts

    def stalling_queue(self):
        if self.patient_mapping_queue is not None:
            limit = (self.patient_mapping_queue.query_time
                     - self.patient_mapping_delay)

            unstalled = set()
            for ((db, mid), ts) in self.stalled_ids.items():
                pid = db.get_patient_id(mid, False)
                if (pid is not None or ts < limit):
                    unstalled.add((db, mid))
            for (db, mid) in unstalled:
                del self.stalled_ids[db, mid]
                self.unstalled_ids.add((db, mid))

            if len(self.stalled_ids) > 0:
                return self.patient_mapping_queue
        return None

class PatientIDExtractorQueue(ExtractorQueue):
    def __init__(self, queue_name, patient_id = None, **kwargs):
        ExtractorQueue.__init__(self, queue_name, **kwargs)
        self.patient_id = patient_id
    def message_channel(self, message):
        return ('P', message.patient_id)
    def message_timestamp(self, message):
        return message.timestamp
    def message_ttl(self, message):
        return self.limit_per_batch * 20
    def default_batch_duration(self):
        return timedelta(minutes = 60)
    def bias(self):
        return timedelta(0)

class WaveSampleQueue(MappingIDExtractorQueue):
    def message_parser(self, db, limit, **kwargs):
        return WaveSampleParser(dialect = db.dialect,
                                paramstyle = db.paramstyle,
                                mapping_id = self.mapping_id,
                                limit = limit,
                                **kwargs)
    def bias(self):
        return timedelta(seconds = -30)
    def idle_delay(self):
        return timedelta(milliseconds = 500)

class NumericValueQueue(MappingIDExtractorQueue):
    def message_parser(self, db, limit, **kwargs):
        return NumericValueParser(dialect = db.dialect,
                                  paramstyle = db.paramstyle,
                                  mapping_id = self.mapping_id,
                                  limit = limit,
                                  **kwargs)
    def idle_delay(self):
        return timedelta(seconds = 1)

class EnumerationValueQueue(MappingIDExtractorQueue):
    def message_parser(self, db, limit, **kwargs):
        return EnumerationValueParser(dialect = db.dialect,
                                      paramstyle = db.paramstyle,
                                      mapping_id = self.mapping_id,
                                      limit = limit,
                                      **kwargs)
    def idle_delay(self):
        return timedelta(milliseconds = 500)

class AlertQueue(MappingIDExtractorQueue):
    def message_parser(self, db, limit, **kwargs):
        return AlertParser(dialect = db.dialect,
                           paramstyle = db.paramstyle,
                           mapping_id = self.mapping_id,
                           limit = limit,
                           **kwargs)
    def idle_delay(self):
        return timedelta(seconds = 1)

class PatientMappingQueue(MappingIDExtractorQueue):
    def message_parser(self, db, limit, **kwargs):
        return PatientMappingParser(dialect = db.dialect,
                                    paramstyle = db.paramstyle,
                                    mapping_id = self.mapping_id,
                                    limit = limit,
                                    **kwargs)
    def bias(self):
        return timedelta(minutes = -8)
    def idle_delay(self):
        return timedelta(minutes = 5)

class PatientBasicInfoQueue(PatientIDExtractorQueue):
    def message_parser(self, db, limit, **kwargs):
        return PatientBasicInfoParser(dialect = db.dialect,
                                      paramstyle = db.paramstyle,
                                      patient_id = self.patient_id,
                                      limit = limit,
                                      **kwargs)
    def idle_delay(self):
        return timedelta(minutes = 31)

class PatientDateAttributeQueue(PatientIDExtractorQueue):
    def message_parser(self, db, limit, **kwargs):
        return PatientDateAttributeParser(dialect = db.dialect,
                                          paramstyle = db.paramstyle,
                                          patient_id = self.patient_id,
                                          limit = limit,
                                          **kwargs)
    def idle_delay(self):
        return timedelta(minutes = 32)

class PatientStringAttributeQueue(PatientIDExtractorQueue):
    def message_parser(self, db, limit, **kwargs):
        return PatientStringAttributeParser(dialect = db.dialect,
                                            paramstyle = db.paramstyle,
                                            patient_id = self.patient_id,
                                            limit = limit,
                                            **kwargs)
    def idle_delay(self):
        return timedelta(minutes = 33)

class BedTagQueue(ExtractorQueue):
    def message_parser(self, db, limit, **kwargs):
        return BedTagParser(dialect = db.dialect,
                            paramstyle = db.paramstyle,
                            limit = limit,
                            **kwargs)
    def message_channel(self, message):
        return None
    def message_timestamp(self, message):
        return message.timestamp
    def message_ttl(self, message):
        return 1000             # XXX
    def idle_delay(self):
        return timedelta(minutes = 34)
