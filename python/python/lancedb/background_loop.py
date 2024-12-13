# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import asyncio
import threading


class BackgroundEventLoop:
    """
    A background event loop that can run futures.

    Used to bridge sync and async code, without messing with users event loops.
    """

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(
            target=self.loop.run_forever,
            name="LanceDBBackgroundEventLoop",
            daemon=True,
        )
        self.thread.start()

    def run(self, future):
        return asyncio.run_coroutine_threadsafe(future, self.loop).result()


LOOP = BackgroundEventLoop()
