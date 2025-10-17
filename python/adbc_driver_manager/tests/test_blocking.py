# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Direct tests of the SIGINT handler.

Higher-level testing of SIGINT during queries appears to be flaky in CI due to
having to send the signal, so this tests the handler itself instead.
"""

import os
import signal
import sys
import threading
import time

import pytest

from adbc_driver_manager import _lib

# https://github.com/apache/arrow-adbc/issues/1522
# It works fine on the normal Windows builds, but not under the Conda builds
# where there is an unexplained/unreplicable crash, and so for now this is
# disabled on Windows
pytestmark = pytest.mark.skipif(os.name == "nt", reason="Disabled on Windows")


def _send_sigint():
    # Windows behavior is different
    # https://stackoverflow.com/questions/35772001
    if os.name == "nt":
        os.kill(os.getpid(), signal.CTRL_C_EVENT)
    else:
        os.kill(os.getpid(), signal.SIGINT)


def _blocking(event):
    _send_sigint()
    event.wait()


def test_sigint_fires():
    # Run the thing that fires SIGINT itself as the "blocking" call
    event = threading.Event()

    def _cancel():
        event.set()

    _lib._blocking_call(_blocking, (event,), {}, _cancel)


def test_handler_restored():
    event = threading.Event()
    _lib._blocking_call(_blocking, (event,), {}, event.set)

    # After it returns, this should raise KeyboardInterrupt like usual
    with pytest.raises(KeyboardInterrupt):
        _blocking(event)
        # Needed on Windows so the handler runs before we exit the block (we
        # won't sleep for the full time)
        time.sleep(60)


def test_args_return():
    def _blocking(a, *, b):
        return a, b

    assert _lib._blocking_call(
        _blocking,
        (1,),
        {"b": 2},
        lambda: None,
    ) == (1, 2)


def test_blocking_raise():
    def _blocking():
        raise ValueError("expected error")

    with pytest.raises(ValueError, match="expected error"):
        _lib._blocking_call(_blocking, (), {}, lambda: None)


def test_cancel_raise():
    event = threading.Event()

    def _blocking(event):
        _send_sigint()
        event.wait()
        # Under freethreaded python, _blocking ends before _cancel finishes
        # and raises the exception, so the exception ends up getting thrown
        # away; sleep a bit to prevent that
        if hasattr(sys, "_is_gil_enabled") and not getattr(sys, "_is_gil_enabled")():
            time.sleep(5)

    def _cancel():
        event.set()
        raise ValueError("expected error")

    with pytest.raises(ValueError, match="expected error"):
        _lib._blocking_call(_blocking, (event,), {}, _cancel)


def test_both_raise():
    event = threading.Event()

    def _blocking(event):
        _send_sigint()
        event.wait()
        raise ValueError("expected error 1")

    def _cancel():
        event.set()
        raise ValueError("expected error 2")

    with pytest.raises(ValueError, match="expected error 1") as excinfo:
        _lib._blocking_call(_blocking, (event,), {}, _cancel)
    assert excinfo.value.__cause__ is not None
    with pytest.raises(ValueError, match="expected error 2"):
        raise excinfo.value.__cause__


def test_nested():
    # To be clear, don't ever do this.
    event = threading.Event()

    def _wrap_blocking():
        _lib._blocking_call(_blocking, (event,), {}, event.set)

    _lib._blocking_call(_wrap_blocking, (), {}, lambda: None)

    # The original handler should be restored
    with pytest.raises(KeyboardInterrupt):
        _send_sigint()
        # Needed on Windows so the handler runs before we exit the block (we
        # won't sleep for the full time)
        time.sleep(60)
