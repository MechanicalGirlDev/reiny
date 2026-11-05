"""Process-scoped native library and callback ownership roots."""

import itertools
import threading


# Native owns a registration until its atomic release token is signalled.
# This root retains callback trampolines, instances, factories AND the CDLL.
_registrations = {}
_registration_lock = threading.RLock()
_tokens = itertools.count(1)
# The ABI's shared runtime is process-scoped, even after all hosts have joined.
_libraries = []
