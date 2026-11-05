"""Named stream and RPC ports on borrowed component contexts."""

import ctypes as C
import json

from ._abi import _U32, _bytes, _text, _uint
from ._handles import Buffer, Handle


class Presence(Handle):
    def next(self, timeout_ns):
        return self._lib._document("presence_next", self._id(), _uint(timeout_ns, positive=True))


class Context(Handle):
    @property
    def info(self):
        return self._lib._document("context_info", self._id())

    @property
    def is_stopping(self):
        value = _U32()
        self._lib._call("context_is_stopping", self._id(), C.byref(value))
        return bool(value.value)

    def wait_stop(self, timeout_ns):
        self._lib._call("context_wait_stop", self._id(), _uint(timeout_ns, positive=True))

    def input(self, name, schema):
        return self._lib._new(Subscription, "context_open_stream", self._id(),
                              _text(name), self._lib._id(schema), 1)

    def output(self, name, schema):
        return self._lib._new(Publisher, "context_open_stream", self._id(),
                              _text(name), self._lib._id(schema), 2)

    def rpc_client(self, name, request_schema, response_schema):
        return self._lib._new(RpcClient, "context_open_rpc", self._id(), _text(name),
                              self._lib._id(request_schema), self._lib._id(response_schema), 1)

    def rpc_server(self, name, request_schema, response_schema):
        return self._lib._new(RpcServer, "context_open_rpc", self._id(), _text(name),
                              self._lib._id(request_schema), self._lib._id(response_schema), 2)

    def watch_input_ready(self, name):
        return self._lib._new(Presence, "context_watch_input_ready", self._id(), _text(name))


class Publisher(Handle):
    def send(self, payload):
        self._lib._call("publisher_send", self._id(), _bytes(payload))


class Subscription(Handle):
    def receive(self, timeout_ns):
        return self._lib._new(Message, "subscription_receive", self._id(),
                              _uint(timeout_ns, positive=True))

    def stats(self, timeout_ns):
        return self._lib._document("subscription_stats", self._id(),
                                   _uint(timeout_ns, positive=True))


class Message(Handle):
    @property
    def payload(self):
        return self._lib._copy("message_copy", self._id(), 0)

    @property
    def source(self):
        return self._lib._copy("message_copy", self._id(), 1)[:-1].decode("utf-8")

    @property
    def source_port(self):
        return self._lib._copy("message_copy", self._id(), 2)[:-1].decode("utf-8")

    @property
    def info(self):
        return json.loads(self._lib._copy("message_copy", self._id(), 3))


class RpcClient(Handle):
    def call_start(self, payload, timeout_ns):
        return self._lib._new(Operation, "rpc_call_start", self._id(), _bytes(payload),
                              _uint(timeout_ns, positive=True))


class Operation(Handle):
    def wait(self, timeout_ns):
        with self._lib._new(Buffer, "operation_wait", self._id(),
                            _uint(timeout_ns, positive=True)) as result:
            return result.bytes


class RpcServer(Handle):
    def receive(self, timeout_ns):
        return self._lib._new(Request, "rpc_server_receive", self._id(),
                              _uint(timeout_ns, positive=True))


class Request(Handle):
    @property
    def payload(self):
        return self._lib._copy("request_copy", self._id())

    def reply(self, payload):
        self._lib._call("request_reply", self._id(), _bytes(payload), 0)

    def reply_error(self, message):
        self._lib._call("request_reply", self._id(), _text(message), 1)
