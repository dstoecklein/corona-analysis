import io
from datetime import datetime
from typing import Union

import eurostat
import pandas as pd
import requests
from pygenesis.py_genesis_client import PyGenesisClient
from pathlib import Path
#from config.core import config_db

def http_request(url: str, decode: bool = True) -> Union[io.StringIO, bytes]:
    response = requests.get(url)
    if decode:
        try:
            return io.StringIO(response.content.decode('utf-8'))
        except UnicodeDecodeError:
            return io.StringIO(response.content.decode('ISO-8859-1'))
    else:
        return response.content


def _handle_german_umlauts_in_columns(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    tmp.columns = tmp.columns.str.replace("Ä", "Ae")
    tmp.columns = tmp.columns.str.replace("Ü", "Ue")
    tmp.columns = tmp.columns.str.replace("Ö", "Oe")
    tmp.columns = tmp.columns.str.replace("ä", "ae")
    tmp.columns = tmp.columns.str.replace("ü", "ue")
    tmp.columns = tmp.columns.str.replace("ö", "oe")
    return tmp


def rki(url: str, purpose: str, save_file: bool, data_type: str, path: Path = None, is_excel: bool = False, sheet_name: str = '') -> pd.DataFrame:
    """
    Reads a given URL from RKI and returns it as a Pandas Dataframe

    :param url: URL from the RKI
    :param purpose: Indicate which purpose the data fullfills (e.g. Tests, R-Value, etc.)
    :param save_file: Determines if the file should be saved as .csv
    :param path: Path in which the file should be saved
    :param is_excel: Determines if given URL is an Excel-file
    :param sheet_name: which Excel-Sheet should be processed
    :return: pd.Dataframe
    """

    if save_file and path is None:
        raise RuntimeError('save_file is true but no path given')
    if not save_file and path is not None:
        raise RuntimeError('Path was given but save_file is false')
    if is_excel and sheet_name == '':
        raise RuntimeError('Excel file given but no Sheet')
    if not is_excel and sheet_name != '':
        raise RuntimeError('Sheet given but no Excel file')

    if is_excel:
        df = pd.read_excel(
            http_request(url, decode=False),
            sheet_name=sheet_name
        )
        df = _handle_german_umlauts_in_columns(df=df)
        df = df[df.filter(regex='^(?!Unnamed)').columns]
    else:
        df = pd.read_csv(
            http_request(url),
            engine='python',
            sep=','
        )
        df = _handle_german_umlauts_in_columns(df=df)

    if save_file:
        filename = datetime.now().strftime(purpose.upper() + '_%Y-%m-%d.' + data_type)
        if data_type == 'ftr':
            df.to_feather(
                path / filename,
                compression='zstd', 
                compression_level=9
            )
        elif data_type == 'csv':
            df.to_csv(
                path / filename,
                sep=",",
                index=False,
                encoding='utf8'
            )

    return df


def estat(code: str, purpose: str, save_file: bool, data_type: str, path: Path = None) -> pd.DataFrame:
    """
    Reads a given Table from Eurostat using eurostat package and returns it as a Pandas Dataframe

    :param code: Code (table name) as per https://ec.europa.eu/
    :param purpose: Should indicate which type of data should be loaded (e.g. Tests, R-Value, etc.)
    :param save_file: Determines if the file should be saved as .csv
    :param path: Path in which the file should be saved
    :return: pd.Dataframe
    """

    if save_file and path is None:
        raise RuntimeError('save_file is true but no path given')
    if not save_file and path is not None:
        raise RuntimeError('Path was given but save_file is false')

    df = eurostat.get_data_df(
        code=code,
        flags=False
    )
    #df = _handle_german_umlauts_in_columns(df=df)

    if save_file:
        filename = datetime.now().strftime(purpose.upper() + '_%Y-%m-%d.' + data_type)
        if data_type == 'ftr':
            df.to_feather(
                path / filename,
                compression='zstd', 
                compression_level=9
            )
        elif data_type == 'csv':
            df.to_csv(
                path / filename,
                sep=",",
                index=False,
                encoding='utf8'
            )
    return df


def divi(url: str, purpose: str, save_file: bool, data_type: str, path: Path = None) -> pd.DataFrame:
    """
    Reads a given URL from DIVI and returns it as a Pandas Dataframe

    :param url: URL from the DIVI
    :param purpose: Should indicate which type of data should be loaded (e.g. Tests, R-Value, etc.)
    :param save_file: Determines if the file should be saved as .csv
    :param path: Path in which the file should be saved
    :return: pd.Dataframe
    """

    if save_file and path is None:
        raise RuntimeError('save_file is true but no path given')
    if not save_file and path is not None:
        raise RuntimeError('Path was given but save_file is false')

    df = pd.read_csv(
        http_request(url),
        engine='python',
        sep=','
    )
    df = _handle_german_umlauts_in_columns(df=df)

    if save_file:
        filename = datetime.now().strftime(purpose.upper() + '_%Y-%m-%d.' + data_type)
        if data_type == 'ftr':
            df.to_feather(
                path / filename,
                compression='zstd', 
                compression_level=9
            )
        elif data_type == 'csv':
            df.to_csv(
                path / filename,
                sep=",",
                index=False,
                encoding='utf8'
            )

    return df

"""
def genesis(code: str, purpose: str, save_file: bool, data_type: str, path: Path = None) -> pd.DataFrame:

    #Reads a given URL from DeStatis and returns it as a Pandas Dataframe

    #:param config_db: Configuration file for the database and Genesis login
    #:param code: Code (table name) as per https://www-genesis.destatis.de/
    #:param purpose: Should indicate which type of data should be loaded (e.g. Tests, R-Value, etc.)
    #:param save_file: Determines if the file should be saved as .csv
    #:param path: Path in which the file should be saved
    #:return: pd.Dataframe


    if save_file and path is None:
        raise RuntimeError('save_file is true but no path given')
    if not save_file and path is not None:
        raise RuntimeError('Path was given but save_file is false')

    client = PyGenesisClient(
        site='DESTATIS',
        username=config_db.genesis_login['username'],
        password=config_db.genesis_login['password']
    )

    df = client.read(code, start_year=1990)
    df = _handle_german_umlauts_in_columns(df=df)
    
    if save_file:
        filename = datetime.now().strftime(purpose.upper() + '_%Y-%m-%d.' + data_type)
        if data_type == 'ftr':
            df.to_feather(
                path / filename,
                compression='zstd', 
                compression_level=9
            )
        elif data_type == 'csv':
            df.to_csv(
                path / filename,
                sep=",",
                index=False,
                encoding='utf8'
            )

    return df
"""

def owid(url: str, purpose: str, save_file: bool, data_type: str, path: Path = None) -> pd.DataFrame:
    """
    Reads a given URL from Our World in Data and returns it as a Pandas Dataframe

    :param url: URL from the Our World in Data
    :param purpose: Indicate which purpose the data fullfills (e.g. Tests, R-Value, etc.)
    :param save_file: Determines if the file should be saved as .csv
    :param path: Path in which the file should be saved
    :return: pd.Dataframe
    """

    if save_file and path is None:
        raise RuntimeError('save_file is true but no path given')
    if not save_file and path is not None:
        raise RuntimeError('Path was given but save_file is false')

    df = pd.read_csv(
        http_request(url),
        engine='python',
        sep=','
    )

    if save_file:
        filename = datetime.now().strftime(purpose.upper() + '_%Y-%m-%d.' + data_type)
        if data_type == 'ftr':
            df.to_feather(
                path / filename,
                compression='zstd', 
                compression_level=9
            )
        elif data_type == 'csv':
            df.to_csv(
                path / filename,
                sep=",",
                index=False,
                encoding='utf8'
            )

    return df
