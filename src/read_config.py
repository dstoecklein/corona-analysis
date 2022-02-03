import yaml


def read_config_file(config_path: str = 'config.yaml') -> dict:
    """
    Reads the .yaml configuration file and returns it as a dictionary.

    :param config_path: Path to the .yaml configuration file
    :return: config_file: The configuration file as dictionary
    """
    with open(config_path) as f:
        config_file = yaml.safe_load(f)
    return config_file
