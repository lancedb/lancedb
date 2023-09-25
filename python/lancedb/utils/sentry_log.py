import sys
import logging
from pathlib import Path
import importlib.metadata
from lancedb.utils import CONFIG
from .general import (
    is_pip_package,
    is_git_dir,
    is_pytest_running,
    is_github_actions_ci,
    is_online,
    ENVIRONMENT,
)


def set_sentry():
    """
    Initialize the Sentry SDK for error tracking and reporting. Only used if sentry_sdk package is installed and
    sync=True in settings. Run 'lancedb settings' to see and update settings YAML file.

    Conditions required to send errors (ALL conditions must be met or no errors will be reported):
        - sentry_sdk package is installed
        - sync=True in  settings
        - pytest is not running
        - running in a pip package installation
        - running in a non-git directory
        - online environment

    The function also configures Sentry SDK to ignore KeyboardInterrupt and FileNotFoundError
    exceptions for now.

    Additionally, the function sets custom tags and user information for Sentry events.
    """

    def before_send(event, hint):
        """
        Modify the event before sending it to Sentry based on specific exception types and messages.

        Args:
            event (dict): The event dictionary containing information about the error.
            hint (dict): A dictionary containing additional information about the error.

        Returns:
            dict: The modified event or None if the event should not be sent to Sentry.
        """
        if "exc_info" in hint:
            exc_type, exc_value, tb = hint["exc_info"]
            if exc_type in (KeyboardInterrupt, FileNotFoundError):
                return None  # do not send event

        event["tags"] = {
            "sys_argv": sys.argv[0],
            "sys_argv_name": Path(sys.argv[0]).name,
            "install": "git"
            if is_git_dir()
            else "pip"
            if is_pip_package()
            else "other",
            "os": ENVIRONMENT,
            "version": importlib.metadata.version("lancedb"),
        }
        return event

    TESTS_RUNNING = is_pytest_running() or is_github_actions_ci()
    ONLINE = is_online()
    if CONFIG["sync"] and not TESTS_RUNNING and ONLINE and is_pip_package():
        # and not is_git_dir(): # not running inside a git dir. Maybe too restrictive?

        # If sentry_sdk package is not installed then return and do not use Sentry
        try:
            import sentry_sdk  # noqa
        except ImportError:
            return

        sentry_sdk.init(
            dsn="https://2a892530e0c92d3a3194cddc897a9090@o4505873570791424.ingest.sentry.io/4505873580425216",
            debug=False,
            traces_sample_rate=1.0,
            environment="production",  # 'dev' or 'production'
            before_send=before_send,
            ignore_errors=[KeyboardInterrupt, FileNotFoundError],
        )
        sentry_sdk.set_user({"id": CONFIG["uuid"]})  # SHA-256 anonymized UUID hash

        # Disable all sentry logging
        for logger in "sentry_sdk", "sentry_sdk.errors":
            logging.getLogger(logger).setLevel(logging.CRITICAL)


set_sentry()
