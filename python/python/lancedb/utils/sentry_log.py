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

import bdb
import importlib.metadata
import logging
import sys
from pathlib import Path

from lancedb.utils import CONFIG

from .general import (
    PLATFORMS,
    TryExcept,
    is_git_dir,
    is_github_actions_ci,
    is_online,
    is_pip_package,
    is_pytest_running,
)


@TryExcept(verbose=False)
def set_sentry():
    """
    Initialize the Sentry SDK for error tracking and reporting. Only used if
    sentry_sdk package is installed and sync=True in settings. Run 'lancedb settings'
    to see and update settings YAML file.

    Conditions required to send errors (ALL conditions must be met or no errors will
    be reported):
        - sentry_sdk package is installed
        - sync=True in  settings
        - pytest is not running
        - running in a pip package installation
        - running in a non-git directory
        - online environment

    The function also configures Sentry SDK to ignore KeyboardInterrupt and
    FileNotFoundError exceptions for now.

    Additionally, the function sets custom tags and user information for Sentry
    events.
    """

    def before_send(event, hint):
        """
        Modify the event before sending it to Sentry based on specific exception
        types and messages.

        Args:
            event (dict): The event dictionary containing information about the error.
            hint (dict): A dictionary containing additional information about
                         the error.

        Returns:
            dict: The modified event or None if the event should not be sent
                  to Sentry.
        """
        if "exc_info" in hint:
            exc_type, exc_value, tb = hint["exc_info"]
            ignored_errors = ["out of memory", "no space left on device", "testing"]
            if any(error in str(exc_value).lower() for error in ignored_errors):
                return None

        if is_git_dir():
            install = "git"
        elif is_pip_package():
            install = "pip"
        else:
            install = "other"

        event["tags"] = {
            "sys_argv": sys.argv[0],
            "sys_argv_name": Path(sys.argv[0]).name,
            "install": install,
            "platforms": PLATFORMS,
            "version": importlib.metadata.version("lancedb"),
        }
        return event

    TESTS_RUNNING = is_pytest_running() or is_github_actions_ci()
    ONLINE = is_online()
    if CONFIG["diagnostics"] and not TESTS_RUNNING and ONLINE and is_pip_package():
        # and not is_git_dir(): # not running inside a git dir. Maybe too restrictive?

        # If sentry_sdk package is not installed then return and do not use Sentry
        try:
            import sentry_sdk  # noqa
        except ImportError:
            return

        sentry_sdk.init(
            dsn="https://c63ef8c64e05d1aa1a96513361f3ca2f@o4505950840946688.ingest.sentry.io/4505950933614592",
            debug=False,
            include_local_variables=False,
            traces_sample_rate=0.5,
            environment="production",  # 'dev' or 'production'
            before_send=before_send,
            ignore_errors=[KeyboardInterrupt, FileNotFoundError, bdb.BdbQuit],
        )
        sentry_sdk.set_user({"id": CONFIG["uuid"]})  # SHA-256 anonymized UUID hash

        # Disable all sentry logging
        for logger in "sentry_sdk", "sentry_sdk.errors":
            logging.getLogger(logger).setLevel(logging.CRITICAL)


set_sentry()
