# Copyright (C) 2023  The CivRealm project
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Simple Web socket client implementation using Tornado framework.
"""

import json
from typing import final
import asyncio
from collections import deque
import hashlib
from pathlib import Path
import traceback
from tornado import httpclient
from tornado import httputil
from tornado import websocket
from tornado import ioloop
from civrealm.freeciv.utils.freeciv_logging import fc_logger

APPLICATION_JSON = "application/json"

DEFAULT_CONNECT_TIMEOUT = 300
DEFAULT_REQUEST_TIMEOUT = 300


class WebSocketWriteError(RuntimeError):
    """A websocket write failed on the websocket's owning IOLoop.

    The write is deliberately not retried: callers must treat a failure after
    dispatch as an uncertain mutation.  ``diagnostic`` is bounded and carries
    no payload.
    """

    def __init__(self, message, *, diagnostic=None):
        super().__init__(message)
        self.diagnostic = diagnostic or {}


class WebSocketClient(object):
    """Base for web socket clients."""

    def __init__(
        self,
        connect_timeout=DEFAULT_CONNECT_TIMEOUT,
        request_timeout=DEFAULT_REQUEST_TIMEOUT,
    ):
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self._ws_connection = None
        self._connection_closed = False
        self._io_loop = None
        self._pending_writes = deque()
        self._write_callback_scheduled = False
        self._deferred_write_error = None
        self._deferred_write_diagnostic = None

    @final
    def connect(self, url):
        """Connect to the server.
        :param str url: server URL.
        """
        self.url = url
        headers = httputil.HTTPHeaders({"Content-Type": APPLICATION_JSON})
        request = httpclient.HTTPRequest(
            url=url,
            connect_timeout=self.connect_timeout,
            request_timeout=self.request_timeout,
            headers=headers,
        )
        asyncio.set_event_loop(asyncio.new_event_loop())
        websocket.websocket_connect(
            request,
            callback=self._connect_callback,
            on_message_callback=self._on_message,
        )
        self._io_loop = ioloop.IOLoop.current()

    @final
    def start_ioloop(self):
        """Start IO/Event loop.

        This method will block current thread, and only return after self.stop_loop() is called in a callback. The function should be started everytime the user wants to resume listening to the server or sending messages to the server. Only after the ioloop is started, the responses from the server (including those for connection and login requests) can be received and the corresponding callbacks can be called.
        """
        if self._connection_closed:
            # This happens when the connection was closed intentionally before the call to start_loop, e.g., the user pressed Ctrl+C to stop the client, or the server closed the connection.
            return
        try:
            self._ensure_owned_asyncio_loop("start_ioloop")
            self._io_loop.start()
            self._raise_deferred_write_error()
        except KeyboardInterrupt:
            raise KeyboardInterrupt

    @final
    def stop_ioloop(self):
        self._io_loop.stop()

    @final
    def get_ioloop(self):
        return self._io_loop

    @final
    def send(self, data):
        """Send message to the server
        :param str data: message.
        """
        if not self._ws_connection:
            fc_logger.debug(
                f"web_socket_client::send: Web socket connection has not been established."
            )
            return
        # A caller from another thread must not be rejected merely because
        # that thread has a different asyncio loop: the actual Tornado call
        # is scheduled on the captured owner below.  Restore the owner only
        # when this thread has no loop at all.
        owned = self._owned_asyncio_loop()
        if owned is not None and owned.is_closed():
            raise RuntimeError("websocket asyncio loop is closed at send")
        try:
            current = asyncio.get_event_loop()
        except RuntimeError:
            current = None
        if current is None and owned is not None:
            asyncio.set_event_loop(owned)
        self._raise_deferred_write_error()
        # TODO: check if we need to clear empty spaces
        msg = json.dumps(data)
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        # Tornado's WebSocketProtocol13.write_message() calls
        # asyncio.ensure_future() synchronously.  Calling it from CivRealm's
        # stopped worker thread therefore fails even when that thread has the
        # right loop installed.  Queue the *actual* call on the owning
        # IOLoop; add_callback preserves FIFO and this path never retries.
        if owned is not None and running is owned:
            self._invoke_write(msg)
        else:
            self._pending_writes.append(msg)
            if not self._write_callback_scheduled:
                self._write_callback_scheduled = True
                try:
                    self._io_loop.add_callback(self._drain_pending_writes)
                except BaseException as exc:
                    self._write_callback_scheduled = False
                    self._pending_writes.clear()
                    self._record_write_failure(exc, "schedule")
                    self._raise_deferred_write_error()

    @final
    def close(self):
        """Close connection."""
        # Connection already closed.
        if self._connection_closed:
            return

        self._ensure_owned_asyncio_loop("close")
            # raise RuntimeError('Web socket connection is already closed.')

        # print(f'Url: {self.url}')
        # loop = ioloop.IOLoop.current()
        # print(f'_ws_connection: {hex(id(self._ws_connection))}')
        # print(f'loop: {hex(id(loop))}\n *******')

        self._connection_closed = True
        # A queued mutation must never run during shutdown.  The callback may
        # already be posted to the owner loop, so _drain_pending_writes also
        # checks this flag when it executes.
        self._pending_writes.clear()
        self._on_connection_close()
        self._ws_connection.close()
        # Use the loop that owns the websocket.  IOLoop.current() can refer
        # to a different (or absent) thread-local loop after a prior RPC.
        self._io_loop.stop()
        self._io_loop.start()
        # ioloop.IOLoop.clear_current()
        # ioloop.IOLoop.current().close()

    @final
    def _connect_callback(self, future):
        if future.exception() is None:
            self._ws_connection = future.result()
            self._on_connection_success()
        else:
            self._on_connection_error(future.exception())

    def _ensure_owned_asyncio_loop(self, stage):
        """Restore or validate the asyncio loop owned by this websocket.

        CivRealm starts and stops the Tornado loop around each RPC.  Tornado
        retains the asyncio loop it was created with, while asyncio's
        thread-local current-loop slot may be cleared between operations.
        Reinstall only that exact loop; never create a replacement loop for a
        live websocket, since futures and callbacks are bound to the owner.
        """
        if self._io_loop is None:
            return
        owned = getattr(self._io_loop, "asyncio_loop", None)
        if owned is None:
            # Tornado versions before the public property used this private
            # name.  Keep compatibility for those versions only.
            owned = getattr(self._io_loop, "_asyncio_loop", None)
        if owned is None:
            return
        if owned.is_closed():
            raise RuntimeError(
                f"websocket asyncio loop is closed at {stage}: "
                f"{self._loop_diagnostic(owned)}")
        try:
            current = asyncio.get_event_loop()
        except RuntimeError:
            current = None
        if current is None:
            asyncio.set_event_loop(owned)
            current = owned
        if current is not owned:
            raise RuntimeError(
                f"websocket asyncio loop changed at {stage}: "
                f"{self._loop_diagnostic(owned, current)}")

    def _owned_asyncio_loop(self):
        if self._io_loop is None:
            return None
        return (getattr(self._io_loop, "asyncio_loop", None)
                or getattr(self._io_loop, "_asyncio_loop", None))

    def _invoke_write(self, msg):
        try:
            result = self._ws_connection.write_message(msg)
        except BaseException as exc:
            self._record_write_failure(exc, "write")
            raise WebSocketWriteError(
                "websocket write failed", diagnostic=self._deferred_write_diagnostic
            ) from exc
        # Tornado normally returns a Future.  Observe asynchronous failures
        # too; an exception in this callback stops the owning loop and is
        # surfaced by start_ioloop after it returns.
        add_done_callback = getattr(result, "add_done_callback", None)
        if callable(add_done_callback):
            try:
                add_done_callback(self._write_future_done)
            except BaseException as exc:
                # The write already crossed the transport boundary; callback
                # registration failure is therefore uncertain and must not
                # cause a replay.
                self._record_write_failure(exc, "write_callback")
                raise WebSocketWriteError(
                    "websocket write callback registration failed",
                    diagnostic=self._deferred_write_diagnostic,
                ) from exc
            # Some test doubles and transports can return an already-failed
            # Future.  Observe it synchronously so a queued FIFO tail is
            # abandoned before the next message is invoked.
            done = getattr(result, "done", None)
            if callable(done) and done():
                self._write_future_done(result)
        return result

    def _write_future_done(self, future):
        try:
            future.result()
        except BaseException as exc:
            self._record_write_failure(exc, "write_future")
            owner = self._owned_asyncio_loop()
            if owner is not None and owner.is_running():
                owner.stop()

    def _drain_pending_writes(self):
        self._write_callback_scheduled = False
        if self._connection_closed:
            self._pending_writes.clear()
            return
        while self._pending_writes and self._deferred_write_error is None:
            msg = self._pending_writes.popleft()
            try:
                self._invoke_write(msg)
            except BaseException:
                # The message was removed before invocation: it is never
                # replayed.  Pending messages are abandoned because their
                # execution order is no longer provable.
                self._pending_writes.clear()
                owner = self._owned_asyncio_loop()
                if owner is not None and owner.is_running():
                    owner.stop()
                return

    def _record_write_failure(self, exc, stage):
        if self._deferred_write_error is not None:
            return
        owned = self._owned_asyncio_loop()
        self._deferred_write_error = exc
        self._pending_writes.clear()
        self._deferred_write_diagnostic = {
            "stage": stage,
            "exception": type(exc).__name__,
            "message": str(exc)[:512],
            "traceback": "".join(traceback.format_exception(
                type(exc), exc, exc.__traceback__, limit=8))[-4096:],
            "owned_loop": self._loop_diagnostic(owned),
        }
        try:
            source = Path(__file__).resolve()
            self._deferred_write_diagnostic["vendor_module"] = str(source)
            self._deferred_write_diagnostic["vendor_module_sha256"] = hashlib.sha256(
                source.read_bytes()).hexdigest()
        except Exception:
            # Provenance is useful but must never mask the transport failure.
            pass

    def _raise_deferred_write_error(self):
        exc = self._deferred_write_error
        if exc is None:
            return
        raise WebSocketWriteError(
            "deferred websocket write failed",
            diagnostic=self._deferred_write_diagnostic,
        ) from exc

    @staticmethod
    def _loop_diagnostic(owned, current=None):
        """Return bounded loop state useful for failure artifacts."""
        def state(loop):
            if loop is None:
                return None
            return {"id": hex(id(loop)),
                    "closed": bool(loop.is_closed()),
                    "running": bool(loop.is_running())}
        return {"owned": state(owned), "current": state(current)}

    def _on_message(self, message):
        """This is called when new message is available from the server.
        :param str msg: server message.
        """
        pass

    def _on_connection_success(self):
        """This is called on successful connection ot the server."""
        pass

    def _on_connection_close(self):
        """This is called when server closed the connection.
        This is called before socket is closed, should be overriden to clean up data before closing the connection.
        """
        pass

    def _on_connection_error(self, exception):
        """This is called in case if connection to the server could
        not established.
        """
        pass
