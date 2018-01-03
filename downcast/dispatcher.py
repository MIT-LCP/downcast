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
import logging

class Dispatcher:
    """Object that tracks and routes incoming messages.

    When a message is submitted, we pass it to each output handler.
    Each handler may decide to accept (ack) the message immediately,
    defer (nack) it, or ignore it.  For each message, we keep track of
    which output handlers have deferred it, and when all such handlers
    have accepted the message, we report this back to the message
    generator.

    We also handle message expiration here: if a message is about to
    expire, we re-submit it to each handler that hasn't yet acked the
    message.  Handlers that need to "look ahead" can use this to
    accomodate the final message, or batch of messages, at the end of
    a patient's stay.

    If the message still has not been acked after notifying handlers
    that it's about to expire, we submit it to the dead-letter handler
    and forcibly ack it ourselves.

    Messages are also sent to the dead-letter handler if no output
    handler explictly accepts or defers them.
    """

    def __init__(self, fatal_exceptions = False):
        self.handlers = []
        self.channels = OrderedDict()
        self.all_messages = OrderedDict()
        self.message_counter = 0
        self.dead_letter_handlers = []
        self.active_handlers = set()
        self.fatal_exceptions = fatal_exceptions

    def add_handler(self, handler):
        """Add a message handler."""
        self.handlers.append(handler)

    def add_dead_letter_handler(self, handler):
        """Add a dead-letter handler."""
        self.dead_letter_handlers.append(handler)

    def send_message(self, channel, msg, source, ttl):
        """Submit a new message.

        The new message is submitted to every handler.  Handlers must
        declare their interest in a particular message by calling
        'ack_message' or 'nack_message'.

        Note that messages must be unique per-channel.

        If the oldest message for the given channel is now about to
        expire, then re-submit it to all interested handlers, and
        delete it afterwards.

        For any handlers that acked or nacked a message, re-submit any
        pending messages from the same channel.
        """

        if self._message_pending(channel, msg):
            self._log_warning('re-sending a known message', msg = msg)
            return

        self._insert_message(channel, msg, source, ttl)

        self.active_handlers = set()

        # Submit the new message to every handler.
        for h in self.handlers:
            self._handler_send_message(h, channel, msg, ttl)
        self._mark_submitted(channel, msg)

        # Check whether any handlers acked or nacked the message.
        if self._message_pending(channel, msg):
            if not self._message_claimed(channel, msg):
                # No handlers were interested.  Drop the message
                # immediately and send it to the dead letter file.
                self._expire_message(channel, msg)
            elif self._message_n_handlers(channel, msg) == 0:
                # All interested handlers acked the message.  Ack it
                # upstream.
                self._delete_message(channel, msg)
                self._source_ack_message(source, channel, msg)
            else:
                # One or more handlers nacked the message.  Nack it
                # upstream.
                self._source_nack_message(source, channel, msg)

        # For any handler that acked or nacked the new message, replay
        # all pending messages from this channel.
        self._replay_pending(channel)

        # Check whether any old messages have now expired.
        self._check_expiring()

    def terminate(self):
        """Force expiration of all pending messages.

        This indicates that the input data stream has ended
        (permanently.)  Note that this should only be used for
        retrospective batch conversion and for testing; in normal
        real-time conversion, this should never be called.  'flush' is
        probably what you're looking for.
        """
        while len(self.all_messages) > 0:
            (channel, msg) = next(iter(self.all_messages))
            self.active_handlers = set()
            self._expire_message(channel, msg)
            self._replay_pending(channel)

    def flush(self):
        """Flush pending output to disk.

        Exactly what this means is left to the discretion of the
        individual message handlers.  However, after this function
        returns, any messages that have been 'acked' may be
        permanently deleted from the input stream.  Thus, handlers
        responsible for archiving real-time data should ensure that
        their output is written to durable storage.
        """
        for h in self.handlers:
            self._handler_flush(h)

    ################################################################

    def ack_message(self, channel, msg, handler):
        """Acknowledge a message.

        This should only be called by message handlers, and only for
        messages that they have received from this dispatcher.
        Calling this function indicates that the given message has
        been fully processed, and may now be discarded upstream.

        (Messages will not be permanently discarded until after a
        'flush', but acking a message represents a promise that,
        following a subsequent 'flush', the message contents will be
        written to disk.)
        """
        if handler not in self.handlers:
            self._log_warning('ack from an unknown handler',
                              handler = handler, msg = msg)

        if not self._message_pending(channel, msg):
            self._log_warning('ack for an unknown message',
                              handler = handler, msg = msg)
        else:
            self._message_del_handler(channel, msg, handler)

            # If all handlers have now acked the message, ack it upstream.
            if (self._message_submitted(channel, msg)
                    and self._message_n_handlers(channel, msg) == 0):
                s = self._message_source(channel, msg)
                self._delete_message(channel, msg)
                self._source_ack_message(s, channel, msg)

    def nack_message(self, channel, msg, handler):
        """Defer processing of a message.

        This should only be called by message handlers, and only for
        messages that they have received from this dispatcher.
        Calling this function indicates that the handler is interested
        in the given message but is not able to process it
        immediately.

        This function is idempotent, and is optional if the handler
        processes messages immediately.  However, either this or
        'ack_message' must be called at least once if the handler
        intends to use the message in the future.
        """
        if handler not in self.handlers:
            self._log_warning('nack from an unknown handler',
                              handler = handler, msg = msg)
        elif not self._message_pending(channel, msg):
            self._log_warning('nack for an unknown message',
                              handler = handler, msg = msg)
        else:
            self._message_add_handler(channel, msg, handler)

    ################################################################

    def _insert_message(self, channel, msg, source, ttl):
        if channel not in self.channels:
            self.channels[channel] = OrderedDict()
        self.channels[channel][msg] = {
            # Source of the message, so we can ack it when we're done
            'source': source,

            # "Time" when the message is due to expire
            'expires': (self.message_counter + ttl),

            # Set of handlers that have nacked the message
            'handlers': set(),

            # Set of handlers that have thrown an error on this message
            'crashed_handlers': set(),

            # Set when all handlers have seen the message
            'submitted': False,

            # Set when any handler has acked/nacked the message
            'claimed': False
        }
        self.all_messages[channel, msg] = 1
        self.message_counter += 1

    def _delete_message(self, channel, msg):
        if channel in self.channels and msg in self.channels[channel]:
            del self.channels[channel][msg]
        if channel in self.channels and len(self.channels[channel]) == 0:
            del self.channels[channel]
        if (channel, msg) in self.all_messages:
            del self.all_messages[channel, msg]

    def _message_pending(self, channel, msg):
        return (channel in self.channels and msg in self.channels[channel])

    def _message_handlers(self, channel, msg):
        for h in self.handlers:
            if self._message_pending(channel, msg):
                if h in self.channels[channel][msg]['handlers']:
                    yield h

    def _message_n_handlers(self, channel, msg):
        if self._message_pending(channel, msg):
            return len(self.channels[channel][msg]['handlers'])
        else:
            return 0

    def _message_add_handler(self, channel, msg, handler):
        if self._message_pending(channel, msg):
            self.channels[channel][msg]['claimed'] = True
            if handler not in self.channels[channel][msg]['handlers']:
                self.channels[channel][msg]['handlers'].add(handler)
                self.active_handlers.add(handler)

    def _message_del_handler(self, channel, msg, handler):
        if self._message_pending(channel, msg):
            self.channels[channel][msg]['claimed'] = True
            if handler in self.channels[channel][msg]['handlers']:
                self.channels[channel][msg]['handlers'].discard(handler)
                self.active_handlers.add(handler)

    def _message_claimed(self, channel, msg):
        if self._message_pending(channel, msg):
            return self.channels[channel][msg]['claimed']
        else:
            return False

    def _message_submitted(self, channel, msg):
        if self._message_pending(channel, msg):
            return self.channels[channel][msg]['submitted']
        else:
            return False

    def _mark_submitted(self, channel, msg):
        if self._message_pending(channel, msg):
            self.channels[channel][msg]['submitted'] = True

    def _message_ttl(self, channel, msg):
        if self._message_pending(channel, msg):
            return (self.channels[channel][msg]['expires']
                    - self.message_counter)
        else:
            return 999999

    def _message_source(self, channel, msg):
        if self._message_pending(channel, msg):
            return self.channels[channel][msg]['source']
        else:
            return None

    def _replay_pending(self, channel):
        while channel in self.channels and len(self.active_handlers) > 0:
            active = self.active_handlers
            self.active_handlers = set()

            # The use of copy(), and the fact that we iterate over all
            # messages here, may be suboptimal.  Try to avoid making
            # this a problem by ensuring that we never keep a huge
            # number of pending messages in any given channel.
            for m in self.channels[channel].copy():
                for h in self._message_handlers(channel, m):
                    if h in active:
                        ttl = self._message_ttl(channel, m)
                        self._handler_send_message(h, channel, m, ttl)

    def _check_expiring(self):
        while len(self.all_messages) > 0:
            # Check if the oldest mesage has now expired.
            # FIXME: this won't work correctly if different messages
            # have different TTLs.  TTL is a bit of a kludge anyway...
            (channel, msg) = next(iter(self.all_messages))
            ttl = self._message_ttl(channel, msg)
            if ttl > 0:
                return
            self.active_handlers = set()
            self._expire_message(channel, msg)
            self._replay_pending(channel)

    def _expire_message(self, channel, msg):
        # Message is about to expire.  Notify all handlers that
        # still have not acked it
        for h in self._message_handlers(channel, msg):
            self._handler_send_message(h, channel, msg, 0)

        # If message still has not been acked, send to dead-letter
        # handlers, delete the message, and ack it upstream.
        if self._message_pending(channel, msg):
            for h in self.dead_letter_handlers:
                self._handler_send_message(h, channel, msg, 0)
            s = self._message_source(channel, msg)
            self._delete_message(channel, msg)
            self._source_ack_message(s, channel, msg)

    ################################################################

    # Note:
    #
    # - OSError and MemoryError are generally fatal.  In daemon mode
    #   it may be desirable to sleep and restart after a while; in
    #   other cases the program should exit.
    #
    # - ImportError, SyntaxError, SystemError, and their subclasses,
    #   are considered fatal bugs, and the program should be halted
    #   immediately.
    #
    # - Other types of exceptions indicate bugs, but not fatal ones;
    #   log the error (once per message) and continue processing.  In
    #   particular, if a handler chokes due to a logical inconsistency
    #   in the input data, this must not interfere with concurrent
    #   processing of unrelated records.
    #
    # 'flush' does not have the context of a particular channel or
    # message, so all exceptions are fatal.  Handlers should not do
    # anything in 'flush' except flushing buffers.

    def _handler_send_message(self, handler, channel, msg, ttl):
        try:
            handler.send_message(channel, msg, self, ttl)
        except (OSError, MemoryError, ImportError, SyntaxError, SystemError):
            raise
        except Exception as e:
            self._log_exception_once(handler, channel, msg, 'send_message', e)

    def _handler_flush(self, handler):
        handler.flush()

    def _source_ack_message(self, source, channel, msg):
        try:
            source.ack_message(channel, msg, self)
        except (OSError, MemoryError, ImportError, SyntaxError, SystemError):
            raise
        except Exception as e:
            self._log_exception_once(source, channel, msg, 'ack_message', e)

    def _source_nack_message(self, source, channel, msg):
        try:
            source.nack_message(channel, msg, self)
        except (OSError, MemoryError, ImportError, SyntaxError, SystemError):
            raise
        except Exception as e:
            self._log_exception_once(source, channel, msg, 'nack_message', e)

    def _log_exception_once(self, handler, channel, msg, text, exc):
        if self.fatal_exceptions:
            raise exc
        if self._message_pending(channel, msg):
            minfo = self.channels[channel][msg]
            if handler not in minfo['crashed_handlers']:
                minfo['crashed_handlers'].add(handler)
                logging.exception('%s [%s]:' % (type(handler).__name__,
                                                type(msg).__name__))

    def _log_warning(self, text, handler = None, msg = None):
        if handler is None and msg is None:
            logging.warning(text)
        elif handler is None:
            logging.warning('[%s]: %s' % (type(msg).__name__, text))
        elif msg is None:
            logging.warning('%s: %s' % (type(handler).__name__, text))
        else:
            logging.warning('%s [%s]: %s' % (type(handler).__name__,
                                             type(msg).__name__, text))
