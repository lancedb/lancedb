import platform
import copy
import os
import hashlib
import uuid
from pathlib import Path
from .general import is_dir_writeable, yaml_load, yaml_save, LOGGER


MACOS, LINUX, WINDOWS = (platform.system() == x for x in ['Darwin', 'Linux', 'Windows'])  # environment booleans

def get_user_config_dir(sub_dir='lancedb'):
    """
    Get the user config directory.

    Args:
        sub_dir (str): The name of the subdirectory to create.

    Returns:
        (Path): The path to the user config directory.
    """
    # Return the appropriate config directory for each operating system
    if WINDOWS:
        path = Path.home() / 'AppData' / 'Roaming' / sub_dir
    elif MACOS:  # macOS
        path = Path.home() / 'Library' / 'Application Support' / sub_dir
    elif LINUX:
        path = Path.home() / '.config' / sub_dir
    else:
        raise ValueError(f'Unsupported operating system: {platform.system()}')

    # GCP and AWS lambda fix, only /tmp is writeable
    if not is_dir_writeable(path.parent):
        LOGGER.warning(f"WARNING ⚠️ user config directory '{path}' is not writeable, defaulting to '/tmp' or CWD."
                       'Alternatively you can define a LANCEDB_CONFIG_DIR environment variable for this path.')
        path = Path('/tmp') / sub_dir if is_dir_writeable('/tmp') else Path().cwd() / sub_dir

    # Create the subdirectory if it does not exist
    path.mkdir(parents=True, exist_ok=True)

    return path

USER_CONFIG_DIR = Path(os.getenv('LANCEDB_CONFIG_DIR') or get_user_config_dir())
CONFIG_FILE = USER_CONFIG_DIR / 'config.yaml'

class Config(dict):
    """
    Manages lancedb config stored in a YAML file.

    Args:
        file (str | Path): Path to the lancedb config YAML file. Default is USER_CONFIG_DIR / 'config.yaml'.
    """

    def __init__(self, file=CONFIG_FILE):
        self.file = Path(file)
        self.defaults = {  # Default global config values
            'sync': True,
            'uuid': hashlib.sha256(str(uuid.getnode()).encode()).hexdigest(),
            }

        super().__init__(copy.deepcopy(self.defaults))

        if not self.file.exists():
            self.save()

        self.load()
        correct_keys = self.keys() == self.defaults.keys()
        correct_types = all(type(a) is type(b) for a, b in zip(self.values(), self.defaults.values()))
        if not (correct_keys and correct_types):
            LOGGER.warning(
                'WARNING ⚠️ LanceDB settings reset to default values. This may be due to a possible problem '
                'with your settings or a recent package update. '
                f"\nView settings with 'lancedb settings' or at '{self.file}'"
                "\nUpdate settings with 'lancedb config key=value', i.e. 'lancedb config runs_dir=path/to/dir'.")
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
