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

from enum import Enum
from multiprocessing import Process, Pipe
import atexit
import traceback
import logging
import cProfile
import os

from .dispatcher import Dispatcher

class ParallelDispatcher:
    """Object that routes messages to a set of child processes.

    When a message is sent to this dispatcher, it is forwarded to one
    of N child processes, selected based on the message channel.  All
    messages in the same channel will be delivered to the same
    process, but no other guarantees are made about how different
    messages will be routed.  Thus, all related messages must be sent
    to the same channel.

    Apart from distributing the workload, and operating
    asynchronously, this class's API is largely compatible with the
    API of the Dispatcher class.
    """

    def __init__(self, n_children, pending_limit = 200, **kwargs):
        self.n_children = n_children
        self.pending_limit = pending_limit
        self.children = None
        self.dispatcher = Dispatcher(**kwargs)

    def add_handler(self, handler):
        """Add a message handler.

        All handlers must be attached before the child processes are
        launched; i.e., before sending any messages.
        """
        if self.children is not None:
            raise Exception('cannot add handlers after sending messages')
        self.dispatcher.add_handler(handler)

    def add_dead_letter_handler(self, handler):
        """Add a dead-letter handler.

        All handlers must be attached before the child processes are
        launched; i.e., before sending any messages.
        """
        if self.children is not None:
            raise Exception('cannot add handlers after sending messages')
        self.dispatcher.add_dead_letter_handler(handler)

    def _start(self):
        if self.children is None:
            self.children = []
            for i in range(0, self.n_children):
                c = ChildConnector(self.dispatcher,
                                   pending_limit = self.pending_limit,
                                   name = ('handler%d' % i))
                self.children.append(c)
            atexit.register(self._stop)

    def _stop(self):
        if self.children is not None:
            atexit.unregister(self._stop)
            for c in self.children:
                c.close()
            self.children = None

    def send_message(self, channel, message, source, ttl):
        """Submit a new message.

        Note that message acknowledgements, as well as exceptions,
        will be reported asynchronously.  In particular, if this
        function raises an exception, it may actually be the result of
        some earlier message.
        """
        self._start()
        k = hash(channel) % self.n_children
        self.children[k].send_message(channel, message, source, ttl)

    def flush(self):
        """Flush pending output to disk.

        Any pending acknowledgements or exceptions will be processed
        before flushing.  If this function raises an exception, it may
        actually be the result of some earlier message.
        """
        self._start()
        for c in self.children:
            c.flush_begin()
        for c in self.children:
            c.flush_end()

    def terminate(self):
        """Force expiration of all pending messages."""
        self._start()
        for c in self.children:
            c.terminate()

class ChildConnector:
    """Object that routes messages to a child process."""

    def __init__(self, handler, pending_limit = 50, name = None):
        self.pending_limit = pending_limit
        self.pending = []
        self.messages = {}
        self.message_id = 0

        (parent_pipe, child_pipe) = Pipe()
        self.child = ChildContext(handler)
        self.process = Process(target = self.child._main,
                               args = (name, parent_pipe, child_pipe),
                               name = name, daemon = True)
        self.process.start()
        self.parent_pipe = parent_pipe
        child_pipe.close()

    def close(self):
        """Shut down the child process."""
        try:
            self._sync_response()
            self.parent_pipe.send(ChildRequest.EXIT)
        except Exception:
            self.parent_pipe.close()
            raise
        self.parent_pipe.close()
        self.process.join()

    def send_message(self, channel, message, source, ttl):
        """Send a message to the child process."""
        if ttl <= 0:
            self._async_message(channel, message, source, ttl)
            self._sync_response()
        else:
            source.nack_message(channel, message, self)
            self._async_message(channel, message, source, ttl)

    def flush_begin(self):
        """Instruct the child process to flush output to disk."""
        self._async_request(ChildRequest.FLUSH)

    def flush_end(self):
        """Wait for the child process to finish flushing output."""
        self._sync_response()

    def terminate(self):
        """Force expiration of all pending messages."""
        self._async_request(ChildRequest.TERMINATE)

    def _async_message(self, channel, message, source, ttl):
        self.message_id += 1
        msgid = self.message_id
        self.messages[msgid] = (channel, message, source)
        self._async_request((msgid, channel, message, ttl))

    def _async_request(self, request):
        if len(self.pending) >= self.pending_limit:
            self._sync_response()
        self.parent_pipe.send(request)
        self.pending.append(request)

    def _sync_response(self):
        self.parent_pipe.send(ChildRequest.SYNC_RESPONSE)
        (acks, exc, exc_msg) = self.parent_pipe.recv()
        for ackid in acks:
            m = self.messages.pop(ackid, None)
            if m is None:
                logging.warning('ack for an unknown message')
            else:
                (channel, message, source) = m
                source.ack_message(channel, message, self)
        if exc is not None:
            if isinstance(exc, BorkedPickleException):
                obj = self.pending[exc.index]
                exc = TypeError('failed to send/recv %r' % (obj,))
            raise exc from Exception(exc_msg)
        self.pending = []

class ChildContext:
    def __init__(self, handler):
        self.handler = handler
        self.message_ids = {}
        self.acks = []
        self.pipe = None

    def _main(self, name, parent_pipe, child_pipe):
        parent_pipe.close()
        self.pipe = child_pipe
        pf = os.environ.get('DOWNCAST_PROFILE_OUT', None)
        if pf is not None and name is not None:
            pf = '%s.%s' % (pf, name)
            cProfile.runctx('self._main1()', globals(), locals(), pf)
        else:
            self._main1()

    def _main1(self):
        try:
            counter = 0
            while True:
                try:
                    req = self.pipe.recv()
                except EOFError:
                    return
                except (OSError, MemoryError):
                    raise
                except Exception as e:
                    raise BorkedPickleException(counter) from e
                counter += 1

                if isinstance(req, tuple):
                    (msgid, channel, message, ttl) = req
                    self.message_ids[channel, message] = msgid
                    self.handler.send_message(channel, message, self, ttl)
                elif req is ChildRequest.SYNC_RESPONSE:
                    resp = (self.acks, None, None)
                    self.acks = []
                    self.pipe.send(resp)
                    counter = 0
                elif req is ChildRequest.FLUSH:
                    self.handler.flush()
                elif req is ChildRequest.TERMINATE:
                    self.handler.terminate()
                elif req is ChildRequest.EXIT:
                    return
        except Exception as exc:
            exc_msg = traceback.format_exc()
            while True:
                try:
                    req = self.pipe.recv()
                except EOFError:
                    return
                if req is ChildRequest.SYNC_RESPONSE:
                    resp = (self.acks, exc, exc_msg)
                    self.acks = []
                    self.pipe.send(resp)

    def nack_message(self, channel, message, handler):
        """Defer processing of a message."""
        pass

    def ack_message(self, channel, message, handler):
        """Acknowledge a message."""
        msgid = self.message_ids.pop((channel, message), None)
        if msgid is None:
            logging.warning('ack for an unknown message')
        else:
            self.acks.append(msgid)

class ChildRequest(Enum):
    SYNC_RESPONSE = 0
    FLUSH = 1
    TERMINATE = 2
    EXIT = 3

class BorkedPickleException(Exception):
    def __init__(self, index):
        self.index = index
