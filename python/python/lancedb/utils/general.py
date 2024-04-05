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

import logging.config
import os

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
