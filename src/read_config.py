import yaml
from pathlib import Path

def read_yaml(file: str = 'config.yaml') -> dict:
    """
    Reads the .yaml configuration file and returns it as a dictionary.

    :param config_path: Path to the .yaml configuration file
    :return: config_file: The configuration file as dictionary
    """
    cwd = Path.cwd()
    path = cwd.parent / file
    with open(path) as f:
        config_file = yaml.safe_load(f)
    return config_file
