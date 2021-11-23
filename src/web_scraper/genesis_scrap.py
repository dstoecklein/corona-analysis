from pygenesis.py_genesis_client import PyGenesisClient
from src.database import config
from src.utils import paths
from datetime import datetime

client = PyGenesisClient(site='DESTATIS', username=config.genesis_username, password=config.genesis_password)
PATH = paths.get_hospitals_ger_path()


def annual_hospitals(save_file: bool):
    df = client.read('23111-0001')

    if save_file:
        file_name = datetime.now().strftime('DESTATIS_HOSP_%Y-%m-%d.csv')

        df.to_csv(
            PATH +
            file_name,
            sep=",",
            encoding='utf8',
            index=False
        )
    return df
