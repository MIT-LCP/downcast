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
from multiprocessing import Process, Pipe, current_process
import atexit
import traceback
import logging
import cProfile
import os
import sys

from .dispatcher import Dispatcher
from .util import setproctitle

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
        sys.excepthook = _handle_fatal_exception

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
            atexit.register(self.shutdown)

    def shutdown(self):
        """Stop all worker processes and wait for them to exit.

        Typically flush should be called first.
        """
        if self.children is not None:
            atexit.unregister(self.shutdown)
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

    _all_pipes = set()

    def __init__(self, handler, pending_limit = 50, name = None):
        self.pending_limit = pending_limit
        self.pending_count = pending_limit
        self.messages = {}
        self.message_id = 0

        (parent_pipe, child_pipe) = Pipe()
        ChildConnector._all_pipes.add(parent_pipe)
        self.child = ChildContext(handler)
        self.process = Process(target = self.child._main,
                               args = (name, child_pipe),
                               name = name)
        self.process.start()
        self.parent_pipe = parent_pipe
        child_pipe.close()

    def close(self):
        """Shut down the child process."""
        try:
            if self.pending_count != self.pending_limit:
                try:
                    self._sync_response()
                except Exception:
                    logging.exception('Unhandled exception in child process')
            self.parent_pipe.send(ChildRequest.EXIT)
        finally:
            self.parent_pipe.close()
            ChildConnector._all_pipes.discard(self.parent_pipe)
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
        if self.pending_count <= 0:
            self._sync_response()
        self.parent_pipe.send(request)
        self.pending_count -= 1

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
                m = self.messages.get(exc.last_seen_msgid + 1, (None, None))
                desc = ('Failed to send/receive a message;' +
                        ' pending channel=%r, message=%r') % (m[0], m[1])
                exc = TypeError(desc)
            raise exc from Exception(exc_msg)
        self.pending_count = self.pending_limit

class ChildContext:
    def __init__(self, handler):
        self.handler = handler
        self.message_ids = {}
        self.acks = []
        self.pipe = None

    def _main(self, name, child_pipe):
        try:
            # Close all of the parent-side pipes that were created
            # previously (and inherited by the child process.)
            # Unfortunately we can't simply close all file
            # descriptors, or even all 'non-inheritable' file
            # descriptors, as that breaks pymssql.
            for p in ChildConnector._all_pipes:
                p.close()
            ChildConnector._all_pipes = set()

            if name is not None:
                setproctitle('downcast:%s' % (name,))

            self.pipe = child_pipe
            pf = os.environ.get('DOWNCAST_PROFILE_OUT', None)
            if pf is not None and name is not None:
                pf = '%s.%s' % (pf, name)
                cProfile.runctx('self._main1()', globals(), locals(), pf)
            else:
                self._main1()
        except:
            _handle_fatal_exception(*sys.exc_info())
            sys.exit(1)

    def _main1(self):
        try:
            msgid = 0
            while True:
                try:
                    req = self.pipe.recv()
                except EOFError:
                    return
                except (OSError, MemoryError):
                    raise
                except Exception as e:
                    # We assume that all other exceptions that occur
                    # here result from an error in the process of
                    # unpickling the message (or, potentially, the
                    # channel.)  Such exceptions can occur even
                    # without raising an exception on the sender side,
                    # and the resulting error message is generally
                    # unhelpful in the extreme.  Thus, we send back an
                    # exception that indicates the *last message ID
                    # that we were able to decode*; the sender, upon
                    # receiving such an exception, can identify the
                    # message that was (probably) the cause of the
                    # exception.
                    #
                    # (We assume that there are never problems with
                    # pickling/unpickling ChildRequests, nor 'msgid'
                    # or 'ttl' values.)
                    raise BorkedPickleException(msgid) from e

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
                except (OSError, MemoryError):
                    raise
                except Exception:
                    req = None
                if req is ChildRequest.SYNC_RESPONSE:
                    resp = (self.acks, exc, exc_msg)
                    self.acks = []
                    self.pipe.send(resp)
                elif req is ChildRequest.EXIT:
                    return

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


def _handle_fatal_exception(exc_type, exc_val, exc_tb):
    if exc_type is not SystemExit:
        hdr = '-------- %s --------\n' % current_process().name
        msg = traceback.format_exception(exc_type, exc_val, exc_tb)
        m = (hdr + ''.join(msg) + '\n').encode(sys.stderr.encoding,
                                               errors = 'replace')
        sys.stderr.flush()
        os.write(sys.stderr.fileno(), m)

class ChildRequest(Enum):
    SYNC_RESPONSE = 0
    FLUSH = 1
    TERMINATE = 2
    EXIT = 3

class BorkedPickleException(Exception):
    def __init__(self, last_seen_msgid):
        self.last_seen_msgid = last_seen_msgid
