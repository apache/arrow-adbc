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

import threading


def _blocking_call(func, args, kwargs, cancel):
    """Run functions that are expected to block off of the main thread."""
    if threading.current_thread() is not threading.main_thread():
        return func(*args, **kwargs)

    ret = None
    exc = None

    def _background_task():
        nonlocal ret
        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            nonlocal exc
            exc = e

    bg = threading.Thread(target=_background_task)
    bg.start()

    try:
        bg.join()
    except KeyboardInterrupt:
        try:
            cancel()
        finally:
            bg.join()
            raise

    if exc:
        raise exc
    return ret
