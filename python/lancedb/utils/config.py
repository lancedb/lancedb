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

import copy
import hashlib
import os
import platform
import uuid
from pathlib import Path

from .general import LOGGER, is_dir_writeable, yaml_load, yaml_save


def get_user_config_dir(sub_dir="lancedb"):
    """
    Get the user config directory.

    Args:
        sub_dir (str): The name of the subdirectory to create.

    Returns:
        (Path): The path to the user config directory.
    """
    # Return the appropriate config directory for each operating system
    if platform.system() == "Windows":
        path = Path.home() / "AppData" / "Roaming" / sub_dir
    elif platform.system() == "Darwin":
        path = Path.home() / "Library" / "Application Support" / sub_dir
    elif platform.system() == "Linux":
        path = Path.home() / ".config" / sub_dir
    else:
        raise ValueError(f"Unsupported operating system: {platform.system()}")

    # GCP and AWS lambda fix, only /tmp is writeable
    if not is_dir_writeable(path.parent):
        LOGGER.warning(
            f"WARNING ⚠️ user config directory '{path}' is not writeable, defaulting "
            "to '/tmp' or CWD. Alternatively you can define a LANCEDB_CONFIG_DIR "
            "environment variable for this path."
        )
        path = (
            Path("/tmp") / sub_dir
            if is_dir_writeable("/tmp")
            else Path().cwd() / sub_dir
        )

    # Create the subdirectory if it does not exist
    path.mkdir(parents=True, exist_ok=True)

    return path


USER_CONFIG_DIR = Path(os.getenv("LANCEDB_CONFIG_DIR") or get_user_config_dir())
CONFIG_FILE = USER_CONFIG_DIR / "config.yaml"


class Config(dict):
    """
    Manages lancedb config stored in a YAML file.

    Args:
        file (str | Path): Path to the lancedb config YAML file. Default is
        USER_CONFIG_DIR / 'config.yaml'.
    """

    def __init__(self, file=CONFIG_FILE):
        self.file = Path(file)
        self.defaults = {  # Default global config values
            "diagnostics": True,
            "uuid": hashlib.sha256(str(uuid.getnode()).encode()).hexdigest(),
        }

        super().__init__(copy.deepcopy(self.defaults))

        if not self.file.exists():
            self.save()

        self.load()
        correct_keys = self.keys() == self.defaults.keys()
        correct_types = all(
            type(a) is type(b) for a, b in zip(self.values(), self.defaults.values())
        )
        if not (correct_keys and correct_types):
            LOGGER.warning(
                "WARNING ⚠️ LanceDB settings reset to default values. This may be due "
                "to a possible problem with your settings or a recent package update. "
                f"\nView settings & usage with 'lancedb settings' or at '{self.file}'"
            )
            self.reset()

    def load(self):
        """Loads settings from the YAML file."""
        super().update(yaml_load(self.file))

    def save(self):
        """Saves the current settings to the YAML file."""
        yaml_save(self.file, dict(self))

    def update(self, *args, **kwargs):
        """Updates a setting value in the current settings."""
        super().update(*args, **kwargs)
        self.save()

    def reset(self):
        """Resets the settings to default and saves them."""
        self.clear()
        self.update(self.defaults)
        self.save()
