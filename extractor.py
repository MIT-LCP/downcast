#
# downcast - tools for unpacking patient data from DWC
#
# Copyright (c) 2017 Laboratory for Computational Physiology
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

from dispatcher import Dispatcher
from parser import (WaveSampleParser, NumericValueParser,
                    EnumerationValueParser, AlertParser,
                    PatientMappingParser, PatientBasicInfoParser,
                    PatientDateAttributeParser,
                    PatientStringAttributeParser, BedTagParser)
from timestamp import very_old_timestamp

class Extractor:
    def __init__(self, db, dest_dir):
        self.db = db
        self.dest_dir = dest_dir
        self.queues = []
        self.dispatcher = Dispatcher()
        self.conn = db.connect()
        self.current_timestamp = very_old_timestamp
        self.queue_timestamp = OrderedDict()

    def add_queue(self, queue):
        self.queues.append(queue)
        self.queue_timestamp[queue] = very_old_timestamp

    def add_handler(self, handler):
        self.dispatcher.add_handler(handler)

    def add_dead_letter_handler(self, handler):
        self.dispatcher.add_dead_letter_handler(handler)

    def run(self):
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
            self._run_queries(q, cursor)
        finally:
            cursor.close()

    def _run_queries(self, queue, cursor):
        parser = queue.next_message_parser(self.db)
        for (query, handler) in parser.queries():
            print(str(query))
            cursor.execute(*query)
            row = cursor.fetchone()
            while row is not None:
                msg = handler(self.db, row)
                if msg is not None:
                    ts = queue.message_timestamp(msg)

                    # FIXME: should disregard timestamps that are
                    # completely absurd (but maybe those should be
                    # thrown away at a lower level.)

                    # current_timestamp = maximum timestamp of any
                    # message we've seen so far
                    if ts > self.current_timestamp:
                        self.current_timestamp = ts

                    # queue_timestamp = maximum timestamp of any
                    # message we've seen in this queue
                    if ts > self.queue_timestamp[queue]:
                        self.queue_timestamp[queue] = ts

                    queue.push_message(msg, self.dispatcher)
                row = cursor.fetchone()

        # If this queue has reached the present time, put it to
        # sleep for some minimum time period before hitting it
        # again.  The delay time is dependent on the queue type.
        if queue.reached_present():
            self.queue_timestamp[queue] = (self.current_timestamp
                                           + queue.idle_delay())

class ExtractorQueue:
    def __init__(self, queue_name, start_time = None):
        self.queue_name = queue_name
        self.newest_seen_timestamp = start_time
        self.oldest_unacked_timestamp = start_time
        self.acked_saved = None
        self.acked_new = OrderedDict()
        self.unacked_new = OrderedDict()
        self.limit_per_batch = 100 # XXX
        self.last_batch_count_at_newest = 0
        self.last_batch_limit = 0
        self.last_batch_count = 0

    def next_message_parser(self, db):
        # this is a bit of a kludge: if batch limit is too small (more
        # than limit/2 messages with exactly the same timestamp),
        # double it until this is no longer true.  possibly better
        # would be to issue a compound query like 'all messages at
        # timestamp T, plus the first N messages at timestamp > T'.

        n = self.limit_per_batch
        while n < (self.last_batch_count_at_newest * 2):
            n *= 2
        self.last_batch_limit = n
        self.last_batch_count = 0
        self.last_batch_count_at_newest = 0
        return self.message_parser(db, self.newest_seen_timestamp, n)

    def reached_present(self):
        # this is nasty.  we want to answer the question "did the
        # previous query end because we reached the limit of available
        # data, or because we reached the batch limit?"  once we've
        # reached the end of available data then we do not want to hit
        # this queue again until all other queues catch up.

        # of course "present" doesn't mean "current time on the system
        # where this code is running" or even "current time on the
        # exporting system", it means "timestamp of data that is
        # currently being inserted into DWC database."

        # XXX determine whether there is any situation under which the
        # query could be aborted without returning all requested
        # results, that would NOT raise an exception.  in such a
        # situation, the queue should not be treated as up-to-date.
        return (self.last_batch_count < self.last_batch_limit)

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
        # Generating and hashing repr(message) may be expensive so
        # don't do it if we don't have to.
        if self.acked_saved is not None and ts in self.acked_saved:
            mstr = repr(message)
            if mstr in self.acked_saved[ts]:
                del self.acked_saved[ts][mstr]
                if len(self.acked_saved[ts]) == 0:
                    del self.acked_saved[ts]
                    if len(self.acked_saved) == 0:
                        self.acked_saved = None
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
        if self.acked_saved is None:
            return
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

################################################################

class MappingIDExtractorQueue(ExtractorQueue):
    def message_channel(self, message):
        return ('M', message.mapping_id)
    def message_timestamp(self, message):
        return message.timestamp
    def message_ttl(self, message):
        return 1000             # XXX

class PatientIDExtractorQueue(ExtractorQueue):
    def message_channel(self, message):
        return ('P', message.patient_id)
    def message_timestamp(self, message):
        return message.timestamp
    def message_ttl(self, message):
        return 1000             # XXX

class WaveSampleQueue(MappingIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return WaveSampleParser(dialect = db.dialect(),
                                paramstyle = db.paramstyle(),
                                time_ge = start_timestamp,
                                limit = limit)
    def idle_delay(self):
        return timedelta(milliseconds = 500)

class NumericValueQueue(MappingIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return NumericValueParser(dialect = db.dialect(),
                                  paramstyle = db.paramstyle(),
                                  time_ge = start_timestamp,
                                  limit = limit)
    def idle_delay(self):
        return timedelta(seconds = 1)

class EnumerationValueQueue(MappingIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return EnumerationValueParser(dialect = db.dialect(),
                                      paramstyle = db.paramstyle(),
                                      time_ge = start_timestamp,
                                      limit = limit)
    def idle_delay(self):
        return timedelta(milliseconds = 500)

class AlertQueue(MappingIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return AlertParser(dialect = db.dialect(),
                           paramstyle = db.paramstyle(),
                           time_ge = start_timestamp,
                           limit = limit)
    def idle_delay(self):
        return timedelta(seconds = 1)

class PatientMappingQueue(MappingIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return PatientMappingParser(dialect = db.dialect(),
                                    paramstyle = db.paramstyle(),
                                    time_ge = start_timestamp,
                                    limit = limit)
    def idle_delay(self):
        return timedelta(minutes = 5)

class PatientBasicInfoQueue(PatientIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return PatientBasicInfoParser(dialect = db.dialect(),
                                      paramstyle = db.paramstyle(),
                                      time_ge = start_timestamp,
                                      limit = limit)
    def idle_delay(self):
        return timedelta(minutes = 31)

class PatientDateAttributeQueue(PatientIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return PatientDateAttributeParser(dialect = db.dialect(),
                                          paramstyle = db.paramstyle(),
                                          time_ge = start_timestamp,
                                          limit = limit)
    def idle_delay(self):
        return timedelta(minutes = 32)

class PatientStringAttributeQueue(PatientIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return PatientStringAttributeParser(dialect = db.dialect(),
                                            paramstyle = db.paramstyle(),
                                            time_ge = start_timestamp,
                                            limit = limit)
    def idle_delay(self):
        return timedelta(minutes = 33)

class BedTagQueue(ExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return BedTagParser(dialect = db.dialect(),
                            paramstyle = db.paramstyle(),
                            time_ge = start_timestamp,
                            limit = limit)
    def message_channel(self, message):
        return None
    def message_timestamp(self, message):
        return message.timestamp
    def message_ttl(self, message):
        return 1000             # XXX
    def idle_delay(self):
        return timedelta(minutes = 34)
