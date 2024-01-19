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

import contextlib
import importlib
import logging.config
import os
import platform
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Union

import requests
import yaml

LOGGING_NAME = "lancedb"
VERBOSE = (
    str(os.getenv("LANCEDB_VERBOSE", True)).lower() == "true"
)  # global verbose mode


def set_logging(name=LOGGING_NAME, verbose=True):
    """Sets up logging for the given name.

    Parameters
    ----------
    name : str, optional
        The name of the logger. Default is 'lancedb'.
    verbose : bool, optional
        Whether to enable verbose logging. Default is True.
    """

    rank = int(os.getenv("RANK", -1))  # rank in world for Multi-GPU trainings
    level = logging.INFO if verbose and rank in {-1, 0} else logging.ERROR
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {name: {"format": "%(message)s"}},
            "handlers": {
                name: {
                    "class": "logging.StreamHandler",
                    "formatter": name,
                    "level": level,
                }
            },
            "loggers": {name: {"level": level, "handlers": [name], "propagate": False}},
        }
    )


set_logging(LOGGING_NAME, verbose=VERBOSE)
LOGGER = logging.getLogger(LOGGING_NAME)


def is_pip_package(filepath: str = __name__) -> bool:
    """Determines if the file at the given filepath is part of a pip package.

    Parameters
    ----------
    filepath : str, optional
        The filepath to check. Default is the current file.

    Returns
    -------
    bool
        True if the file is part of a pip package, False otherwise.
    """
    # Get the spec for the module
    spec = importlib.util.find_spec(filepath)

    # Return whether the spec is not None and the origin is not None (indicating
    # it is a package)
    return spec is not None and spec.origin is not None


def is_pytest_running():
    """Determines whether pytest is currently running or not.

    Returns
    -------
    bool
        True if pytest is running, False otherwise.
    """
    return (
        ("PYTEST_CURRENT_TEST" in os.environ)
        or ("pytest" in sys.modules)
        or ("pytest" in Path(sys.argv[0]).stem)
    )


def is_github_actions_ci() -> bool:
    """
    Determine if the current environment is a GitHub Actions CI Python runner.

    Returns
    -------
    bool
        True if the current environment is a GitHub Actions CI Python runner,
        False otherwise.
    """

    return (
        "GITHUB_ACTIONS" in os.environ
        and "RUNNER_OS" in os.environ
        and "RUNNER_TOOL_CACHE" in os.environ
    )


def is_git_dir():
    """
    Determines whether the current file is part of a git repository.
    If the current file is not part of a git repository, returns None.

    Returns
    -------
    bool
        True if current file is part of a git repository.
    """
    return get_git_dir() is not None


def is_online() -> bool:
    """
    Check internet connectivity by attempting to connect to a known online host.

    Returns
    -------
    bool
        True if connection is successful, False otherwise.
    """
    import socket

    for host in "1.1.1.1", "8.8.8.8", "223.5.5.5":  # Cloudflare, Google, AliDNS:
        try:
            test_connection = socket.create_connection(address=(host, 53), timeout=2)
        except (socket.timeout, socket.gaierror, OSError):  # noqa: PERF203
            continue
        else:
            # If the connection was successful, close it to avoid a ResourceWarning
            test_connection.close()
            return True
    return False


def is_dir_writeable(dir_path: Union[str, Path]) -> bool:
    """Check if a directory is writeable.

    Parameters
    ----------
    dir_path : Union[str, Path]
        The path to the directory.

    Returns
    -------
    bool
        True if the directory is writeable, False otherwise.
    """
    return os.access(str(dir_path), os.W_OK)


def is_colab():
    """Check if the current script is running inside a Google Colab notebook.

    Returns
    -------
    bool
        True if running inside a Colab notebook, False otherwise.
    """
    return "COLAB_RELEASE_TAG" in os.environ or "COLAB_BACKEND_VERSION" in os.environ


def is_kaggle():
    """Check if the current script is running inside a Kaggle kernel.

    Returns
    -------
    bool
        True if running inside a Kaggle kernel, False otherwise.
    """
    return (
        os.environ.get("PWD") == "/kaggle/working"
        and os.environ.get("KAGGLE_URL_BASE") == "https://www.kaggle.com"
    )


def is_jupyter():
    """Check if the current script is running inside a Jupyter Notebook.

    Returns
    -------
    bool
        True if running inside a Jupyter Notebook, False otherwise.
    """
    with contextlib.suppress(Exception):
        from IPython import get_ipython

        return get_ipython() is not None
    return False


def is_docker() -> bool:
    """Determine if the script is running inside a Docker container.

    Returns
    -------
    bool
        True if the script is running inside a Docker container, False otherwise.
    """
    file = Path("/proc/self/cgroup")
    if file.exists():
        with open(file) as f:
            return "docker" in f.read()
    else:
        return False


def get_git_dir():
    """Determine whether the current file is part of a git repository and if so,
    returns the repository root directory.
    If the current file is not part of a git repository, returns None.

    Returns
    -------
    Path | None
        Git root directory if found or None if not found.
    """
    for d in Path(__file__).parents:
        if (d / ".git").is_dir():
            return d


def get_git_origin_url():
    """Retrieve the origin URL of a git repository.

    Returns
    -------
    str | None
        The origin URL of the git repository or None if not git directory.
    """
    if is_git_dir():
        with contextlib.suppress(subprocess.CalledProcessError):
            origin = subprocess.check_output(
                ["git", "config", "--get", "remote.origin.url"]
            )
            return origin.decode().strip()


def yaml_save(file="data.yaml", data=None, header=""):
    """Save YAML data to a file.

    Parameters
    ----------
    file : str, optional
        File name, by default 'data.yaml'.
    data : dict, optional
        Data to save in YAML format, by default None.
    header : str, optional
        YAML header to add, by default "".
    """
    if data is None:
        data = {}
    file = Path(file)
    if not file.parent.exists():
        # Create parent directories if they don't exist
        file.parent.mkdir(parents=True, exist_ok=True)

    # Convert Path objects to strings
    for k, v in data.items():
        if isinstance(v, Path):
            data[k] = str(v)

    # Dump data to file in YAML format
    with open(file, "w", errors="ignore", encoding="utf-8") as f:
        if header:
            f.write(header)
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)


def yaml_load(file="data.yaml", append_filename=False):
    """
    Load YAML data from a file.

    Parameters
    ----------
    file : str, optional
        File name. Default is 'data.yaml'.
    append_filename : bool, optional
        Add the YAML filename to the YAML dictionary. Default is False.

    Returns
    -------
    dict
        YAML data and file name.
    """
    assert Path(file).suffix in (
        ".yaml",
        ".yml",
    ), f"Attempting to load non-YAML file {file} with yaml_load()"
    with open(file, errors="ignore", encoding="utf-8") as f:
        s = f.read()  # string

        # Add YAML filename to dict and return
        data = (
            yaml.safe_load(s) or {}
        )  # always return a dict (yaml.safe_load() may return None for empty files)
        if append_filename:
            data["yaml_file"] = str(file)
        return data


def yaml_print(yaml_file: Union[str, Path, dict]) -> None:
    """
    Pretty prints a YAML file or a YAML-formatted dictionary.

    Parameters
    ----------
    yaml_file : Union[str, Path, dict]
        The file path of the YAML file or a YAML-formatted dictionary.

    Returns
    -------
    None
    """
    yaml_dict = (
        yaml_load(yaml_file) if isinstance(yaml_file, (str, Path)) else yaml_file
    )
    dump = yaml.dump(yaml_dict, sort_keys=False, allow_unicode=True)
    LOGGER.info("Printing '%s'\n\n%s", yaml_file, dump)


PLATFORMS = [platform.system()]
if is_colab():
    PLATFORMS.append("Colab")
if is_kaggle():
    PLATFORMS.append("Kaggle")
if is_jupyter():
    PLATFORMS.append("Jupyter")
if is_docker():
    PLATFORMS.append("Docker")

PLATFORMS = "|".join(PLATFORMS)


class TryExcept(contextlib.ContextDecorator):
    """
    TryExcept context manager.
    Usage: @TryExcept() decorator or 'with TryExcept():' context manager.
    """

    def __init__(self, msg="", verbose=True):
        """
        Parameters
        ----------
        msg : str, optional
            Custom message to display in case of exception, by default "".
        verbose : bool, optional
            Whether to display the message, by default True.
        """
        self.msg = msg
        self.verbose = verbose

    def __enter__(self):
        pass

    def __exit__(self, exc_type, value, traceback):
        if self.verbose and value:
            LOGGER.info("%s%s%s", self.msg, ": " if self.msg else "", value)
        return True


def threaded_request(
    method, url, retry=3, timeout=30, thread=True, code=-1, verbose=True, **kwargs
):
    """
    Makes an HTTP request using the 'requests' library, with exponential backoff
    retries up to a specified timeout.

    Parameters
    ----------
    method : str
        The HTTP method to use for the request. Choices are 'post' and 'get'.
    url : str
        The URL to make the request to.
    retry : int, optional
        Number of retries to attempt before giving up, by default 3.
    timeout : int, optional
        Timeout in seconds after which the function will give up retrying,
        by default 30.
    thread : bool, optional
        Whether to execute the request in a separate daemon thread, by default True.
    code : int, optional
        An identifier for the request, used for logging purposes, by default -1.
    verbose : bool, optional
        A flag to determine whether to print out to console or not, by default True.

    Returns
    -------
    requests.Response
        The HTTP response object. If the request is executed in a separate thread,
        returns the thread itself.
    """
    # retry only these codes TODO: add codes if needed in future (500, 408)
    retry_codes = ()

    @TryExcept(verbose=verbose)
    def func(method, url, **kwargs):
        """Make HTTP requests with retries and timeouts, with optional progress
        tracking.
        """
        response = None
        t0 = time.time()
        for i in range(retry + 1):
            if (time.time() - t0) > timeout:
                break
            response = requests.request(method, url, **kwargs)
            if response.status_code < 300:  # good return codes in the 2xx range
                break
            try:
                m = response.json().get("message", "No JSON message.")
            except AttributeError:
                m = "Unable to read JSON."
            if i == 0:
                if response.status_code in retry_codes:
                    m += f" Retrying {retry}x for {timeout}s." if retry else ""
                elif response.status_code == 429:  # rate limit
                    m = "Rate limit reached"
                if verbose:
                    LOGGER.warning("%s #%s", response.status_code, m)
                if response.status_code not in retry_codes:
                    return response
            time.sleep(2**i)  # exponential standoff
        return response

    args = method, url
    if thread:
        return threading.Thread(
            target=func, args=args, kwargs=kwargs, daemon=True
        ).start()
    else:
        return func(*args, **kwargs)
