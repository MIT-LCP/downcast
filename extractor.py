from collections import OrderedDict
import heapq

from dispatcher import Dispatcher
from parser import (WaveSampleParser, NumericValueParser,
                    EnumerationValueParser, AlertParser,
                    PatientMappingParser, PatientBasicInfoParser,
                    PatientDateAttributeParser,
                    PatientStringAttributeParser, BedTagParser)

class Extractor:
    def __init__(self, db, dest_dir):
        self.db = db
        self.dest_dir = dest_dir
        self.queues = []
        self.dispatcher = Dispatcher()
        self.conns = {}

    def add_queue(self, queue):
        self.queues.append(queue)

    def add_handler(self, handler):
        self.dispatcher.add_handler(handler)

    def add_dead_letter_handler(self, handler):
        self.dispatcher.add_dead_letter_handler(handler)

    def run(self):
        lists = []
        for q in self.queues:
            if q not in self.conns:
                self.conns[q] = self.db.connect()
            lists.append(self._messages(self.conns[q], q, len(lists)))

        # Repeatedly pull the oldest message, and submit it via the
        # appropriate queue
        for (ts, ind, q, msg) in heapq.merge(*lists):
            q.push_message(msg, self.dispatcher)

    def _messages(self, conn, q, index):
        # FIXME: this should actually keep going until it reaches the
        # present time.  make it finite here, just for testing...
        parser = q.next_message_parser(self.db)
        for (query, handler) in parser.queries():
            cursor = conn.cursor()
            print(str(query))
            cursor.execute(*query)
            row = cursor.fetchone()
            while row is not None:
                msg = handler(self.db, row)
                if msg is not None:
                    yield (q.message_timestamp(msg), index, q, msg)
                row = cursor.fetchone()
            cursor.close()

class ExtractorQueue:
    def __init__(self, queue_name):
        self.queue_name = queue_name
        self.newest_seen_timestamp = None
        self.oldest_unacked_timestamp = None
        self.acked_saved = None
        self.acked_new = OrderedDict()
        self.unacked_new = OrderedDict()
        self.limit_per_batch = 100 # XXX

    def next_message_parser(self, db):
        # FIXME: this will fail badly if there are more than
        # 'limit_per_batch' messages at the same timestamp.  we need
        # to keep track of how many messages we saw last time, and if
        # necessary, issue a compound query like 'all messages at
        # timestamp T, plus the first N messages at timestamp > T'.
        return self.message_parser(db, self.newest_seen_timestamp,
                                   self.limit_per_batch)

    def push_message(self, message, dispatcher):
        ts = self.message_timestamp(message)
        channel = self.message_channel(message)
        ttl = self.message_ttl(message)

        if self.newest_seen_timestamp is not None:
            if ts < self.newest_seen_timestamp:
                # FIXME: in case of bad weirdness, maybe what we want
                # here is to send the message immediately, with ttl of
                # zero (and dispatcher could recognize that case
                # specifically.)
                self._log_warning('Unexpected message at %s; ignored' % ts)
                return

        self.newest_seen_timestamp = ts
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

class NumericValueQueue(MappingIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return NumericValueParser(dialect = db.dialect(),
                                  paramstyle = db.paramstyle(),
                                  time_ge = start_timestamp,
                                  limit = limit)

class EnumerationValueQueue(MappingIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return EnumerationValueParser(dialect = db.dialect(),
                                      paramstyle = db.paramstyle(),
                                      time_ge = start_timestamp,
                                      limit = limit)

class AlertQueue(MappingIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return AlertParser(dialect = db.dialect(),
                           paramstyle = db.paramstyle(),
                           time_ge = start_timestamp,
                           limit = limit)

class PatientMappingQueue(MappingIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return PatientMappingParser(dialect = db.dialect(),
                                    paramstyle = db.paramstyle(),
                                    time_ge = start_timestamp,
                                    limit = limit)

class PatientBasicInfoQueue(PatientIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return PatientBasicInfoParser(dialect = db.dialect(),
                                      paramstyle = db.paramstyle(),
                                      time_ge = start_timestamp,
                                      limit = limit)

class PatientDateAttributeQueue(PatientIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return PatientDateAttributeParser(dialect = db.dialect(),
                                          paramstyle = db.paramstyle(),
                                          time_ge = start_timestamp,
                                          limit = limit)

class PatientStringAttributeQueue(PatientIDExtractorQueue):
    def message_parser(self, db, start_timestamp, limit):
        return PatientStringAttributeParser(dialect = db.dialect(),
                                            paramstyle = db.paramstyle(),
                                            time_ge = start_timestamp,
                                            limit = limit)

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
