#  Copyright 2023 LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import datetime
import importlib.metadata
import platform
import random
import sys
import time

from lancedb.utils import CONFIG
from lancedb.utils.general import TryExcept

from .general import (
    PLATFORMS,
    get_git_origin_url,
    is_git_dir,
    is_github_actions_ci,
    is_online,
    is_pip_package,
    is_pytest_running,
    threaded_request,
)


class _Events:
    """
    A class for collecting anonymous event analytics. Event analytics are enabled when
    ``diagnostics=True`` in config and disabled when ``diagnostics=False``.

    You can enable or disable diagnostics by running ``lancedb diagnostics --enabled``
    or ``lancedb diagnostics --disabled``.

    Attributes
    ----------
    url : str
        The URL to send anonymous events.
    rate_limit : float
        The rate limit in seconds for sending events.
    metadata : dict
        A dictionary containing metadata about the environment.
    enabled : bool
        A flag to enable or disable Events based on certain conditions.
    """

    _instance = None

    url = "https://app.posthog.com/capture/"
    headers = {"Content-Type": "application/json"}
    api_key = "phc_oENDjGgHtmIDrV6puUiFem2RB4JA8gGWulfdulmMdZP"
    # This api-key is write only and is safe to expose in the codebase.

    def __init__(self):
        """
        Initializes the Events object with default values for events, rate_limit,
        and metadata.
        """
        self.events = []  # events list
        self.throttled_event_names = ["search_table"]
        self.throttled_events = set()
        self.max_events = 5  # max events to store in memory
        self.rate_limit = 60.0 * 5  # rate limit (seconds)
        self.time = 0.0

        if is_git_dir():
            install = "git"
        elif is_pip_package():
            install = "pip"
        else:
            install = "other"
        self.metadata = {
            "cli": sys.argv[0],
            "install": install,
            "python": ".".join(platform.python_version_tuple()[:2]),
            "version": importlib.metadata.version("lancedb"),
            "platforms": PLATFORMS,
            "session_id": round(random.random() * 1e15),
            # TODO: In future we might be interested in this metric
            # 'engagement_time_msec': 1000
        }

        TESTS_RUNNING = is_pytest_running() or is_github_actions_ci()
        ONLINE = is_online()
        self.enabled = (
            CONFIG["diagnostics"]
            and not TESTS_RUNNING
            and ONLINE
            and (
                is_pip_package()
                or get_git_origin_url() == "https://github.com/lancedb/lancedb.git"
            )
        )

    def __call__(self, event_name, params={}):
        """
        Attempts to add a new event to the events list and send events if the rate
        limit is reached.

        Args
        ----
        event_name : str
            The name of the event to be logged.
        params : dict, optional
            A dictionary of additional parameters to be logged with the event.
        """
        ### NOTE: We might need a way to tag a session with a label to check usage
        ### from a source. Setting label should be exposed to the user.
        if not self.enabled:
            return
        if (
            len(self.events) < self.max_events
        ):  # Events list limited to self.max_events (drop any events past this)
            params.update(self.metadata)
            event = {
                "event": event_name,
                "properties": params,
                "timestamp": datetime.datetime.now(
                    tz=datetime.timezone.utc
                ).isoformat(),
                "distinct_id": CONFIG["uuid"],
            }
            if event_name not in self.throttled_event_names:
                self.events.append(event)
            elif event_name not in self.throttled_events:
                self.throttled_events.add(event_name)
                self.events.append(event)

        # Check rate limit
        t = time.time()
        if (t - self.time) < self.rate_limit:
            return
        # Time is over rate limiter, send now
        data = {
            "api_key": self.api_key,
            "distinct_id": CONFIG["uuid"],  # posthog needs this to accepts the event
            "batch": self.events,
        }
        # POST equivalent to requests.post(self.url, json=data).
        # threaded request is used to avoid blocking, retries are disabled, and
        # verbose is disabled to avoid any possible disruption in the console.
        threaded_request(
            method="post",
            url=self.url,
            headers=self.headers,
            json=data,
            retry=0,
            verbose=False,
        )

        # Flush & Reset
        self.events = []
        self.throttled_events = set()
        self.time = t


@TryExcept(verbose=False)
def register_event(name: str, **kwargs):
    if _Events._instance is None:
        _Events._instance = _Events()

    _Events._instance(name, **kwargs)
