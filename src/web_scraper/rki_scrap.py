import pandas as pd
from datetime import datetime
from src.utils import paths, web_scrap_helper

# constants
HEADER = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/87.0.4280.88 Safari/537.36",
          "X-Requested-With": "XMLHttpRequest"}

URL_COVID = 'https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74/data'
URL_TESTS = 'https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Testzahlen-gesamt.xlsx?__blob=publicationFile'
URL_TESTS_STATES = 'https://ars.rki.de/Docs/SARS_CoV2/Daten/data_wochenbericht.xlsx'
URL_RVALUE = 'https://raw.githubusercontent.com/robert-koch-institut/SARS-CoV-2-Nowcasting_und_-R-Schaetzung/main/Nowcast_R_aktuell.csv'

PATH = paths.get_covid19_ger_path()


def covid_daily(save_file: bool):
    df = pd.read_csv(
        web_scrap_helper.http_request(URL_COVID),
        engine='python',
        sep=','
    )

    if save_file:
        file_name = datetime.now().strftime('RKI_COVID19_%Y-%m-%d.csv')

        df.to_csv(
            PATH +
            file_name,
            sep=",",
            encoding='utf8',
            index=False
        )
    return df


def rvalue_daily(save_file: bool):
    df = pd.read_csv(
        web_scrap_helper.http_request(URL_RVALUE),
        engine='python',
        sep=','
    )

    if save_file:
        file_name = datetime.now().strftime('RKI_RVALUE_%Y-%m-%d.csv.xz')

        df.to_csv(
            PATH +
            file_name,
            sep=",",
            encoding='utf8',
            index=False
        )
    return df


def tests_weekly(save_file: bool):
    df = pd.read_excel(
        web_scrap_helper.http_request(URL_TESTS, decode=False),
        sheet_name='1_Testzahlerfassung'
    )
    df = df[df.filter(regex='^(?!Unnamed)').columns]

    if save_file:
        file_name = datetime.now().strftime('RKI_TESTS_%Y-%m-%d.csv')

        df.to_csv(
            PATH +
            file_name,
            sep=",",
            encoding='utf8',
            index=False
        )
    return df


def tests_weekly_states(save_file: bool):
    df = pd.read_excel(
        web_scrap_helper.http_request(URL_TESTS_STATES, decode=False),
        sheet_name='Abb. 3 Bundesland',
    )

    df = df[df.filter(regex='^(?!Unnamed)').columns]

    if save_file:
        file_name = datetime.now().strftime('RKI_TESTS_STATES_%Y-%m-%d.csv')

        df.to_csv(
            PATH +
            file_name,
            sep=",",
            encoding='utf8',
            index=False
        )
    return df
