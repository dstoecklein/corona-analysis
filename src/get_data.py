import io
import pandas as pd
import requests
from datetime import datetime
from typing import Union


def http_request(url: str, decode: bool = True) -> Union[io.StringIO, bytes]:
    response = requests.get(url)
    if decode:
        try:
            return io.StringIO(response.content.decode('utf-8'))
        except UnicodeDecodeError:
            return io.StringIO(response.content.decode('ISO-8859-1'))
    else:
        return response.content


def rki(url: str, purpose: str, save_file: bool, path: str, is_excel: bool = False, sheet_name: str = '') -> pd.DataFrame:
    """
    Reads a given URL from RKI and returns it as Pandas Dataframe

    :param url: URL from the RKI
    :param purpose: Should indicate which type of data should be loaded (e.g. Tests, R-Value, etc.)
    :param save_file: Determines if the file should be saved as .csv
    :param path: Path in which the file should be saved
    :param is_excel: Determines if given URL is an Excel-file
    :param sheet_name: which Excel-Sheet should be processed
    :return: pd.Dataframe
    """

    if save_file and path == '':
        raise RuntimeError('save_file is true but no path given')
    if not save_file and path != '':
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
        df = df[df.filter(regex='^(?!Unnamed)').columns]
    else:
        df = pd.read_csv(
            http_request(url),
            engine='python',
            sep=','
        )

    if save_file:
        filename = datetime.now().strftime(purpose + '_%Y-%m-%d.csv')

        df.to_csv(
            path + filename,
            sep=',',
            encoding='utf8',
            index=False
        )

    return df
