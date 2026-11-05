"""Explicit trusted service components over reiny_component.h ABI v1.

Load a matching native library with Library(path). All timeouts are positive
integer nanoseconds. No steps, jobs, deployment owner, or zero-copy is implied.
"""

import ctypes as C
from enum import IntEnum
import itertools
import json
import threading
import traceback


from ._abi import (
    Status, Phase, StopReason, ReinyError, DeadlineError, ClosedError, RemoteError,
    NoReplyError, SchemaError, AlreadyCompletedError, _ERRORS, _U64, _U32, _PTR, _OUT,
    _Bytes, _Create, _Hook, _Stop, _Destroy, _Factory, _bytes, _text, _uint,
)
from ._handles import (
    Handle, Buffer, Schema, _FactoryReleaseToken, Graph,
)
from ._lifecycle import (
    Component, _Registration, Registry, Host, Observer,
)
from ._ports import (
    Presence, Context, Publisher, Subscription, Message, RpcClient, Operation,
    RpcServer, Request,
)
from ._library import (
    Library,
)
from ._ownership import (
    _libraries, _registrations, _registration_lock, _tokens,
)

# Keep the published module identity for pickles and class introspection.
for _export in (
    Status, Phase, StopReason, ReinyError, DeadlineError, ClosedError, RemoteError,
    NoReplyError, SchemaError, AlreadyCompletedError, _Bytes, _Factory, Library,
    Handle, Buffer, Schema, _FactoryReleaseToken, Graph, Component, _Registration,
    Registry, Host, Observer, Presence, Context, Publisher, Subscription, Message,
    RpcClient, Operation, RpcServer, Request, _bytes, _text, _uint,
):
    _export.__module__ = __name__
del _export
