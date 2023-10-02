import datetime
import importlib.metadata
import platform
import random
import sys
import time

from lancedb.utils import CONFIG

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


class Events:
    """
    A class for collecting anonymous event analytics. Event analytics are enabled when sync=True in config and
    disabled when sync=False.

    Attributes:
        url (str): The URL to send anonymous events.
        rate_limit (float): The rate limit in seconds for sending events.
        metadata (dict): A dictionary containing metadata about the environment.
        enabled (bool): A flag to enable or disable Events based on certain conditions.
    """

    url = "https://app.posthog.com/capture/"
    headers = {"Content-Type": "application/json"}
    api_key = "phc_oENDjGgHtmIDrV6puUiFem2RB4JA8gGWulfdulmMdZP"
    # This api-key is write only and is safe to expose in the codebase.

    def __new__(cls):
        # Ensure singleton
        if not hasattr(cls, "instance"):
            cls.instance = super(Events, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        """
        Initializes the Events object with default values for events, rate_limit, and metadata.
        """
        self.events = []  # events list
        self.rate_limit = 60.0  # rate limit (seconds)
        self.time = 0.0
        self.metadata = {
            "cli": sys.argv[0],
            "install": "git"
            if is_git_dir()
            else "pip"
            if is_pip_package()
            else "other",
            "python": ".".join(platform.python_version_tuple()[:2]),
            "version": importlib.metadata.version("lancedb"),
            "platforms": PLATFORMS,
            "session_id": round(random.random() * 1e15),
            # 'engagement_time_msec': 1000 # TODO: In future we might be interested in this metric
        }

        TESTS_RUNNING = is_pytest_running() or is_github_actions_ci()
        ONLINE = is_online()
        self.enabled = (
            CONFIG["sync"]
            and not TESTS_RUNNING
            and ONLINE
            and (
                is_pip_package()
                or get_git_origin_url() == "https://github.com/lancedb/lancedb.git"
            )
        )

    def __call__(self, event_name, params={}):
        """
        Attempts to add a new event to the events list and send events if the rate limit is reached.

        Args:

        """
        ### NOTE: We might need a way to tag a session with a label to check usage from a source. Setting label should be exposed to the user.
        if not self.enabled:
            return

        if (
            len(self.events) < 25
        ):  # Events list limited to 25 events (drop any events past this)
            params["metadata"] = self.metadata
            self.events.append(
                {
                    "event": event_name,
                    "properties": params,
                    "timestamp": datetime.datetime.now(
                        tz=datetime.timezone.utc
                    ).isoformat(),
                    "distinct_id": CONFIG["uuid"],
                }
            )

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
        # threaded request is used to avoid blocking, retries are disabled, and verbose is disabled
        # to avoid any possible disruption in the console.
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
        self.time = t


EVENTS = Events()
