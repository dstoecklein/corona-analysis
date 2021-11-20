import pandas as pd
from datetime import datetime
from src.utils import paths
from src.web_scraper import web_scrap_helper

# constants
HEADER = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/87.0.4280.88 Safari/537.36",
          "X-Requested-With": "XMLHttpRequest"}

URL_DIVI = 'https://diviexchange.blob.core.windows.net/%24web/zeitreihe-tagesdaten.csv'

PATH = paths.get_hospitals_ger_path()


def daily_itcu(save_file: bool):
    df = pd.read_csv(
        web_scrap_helper.http_request(URL_DIVI),
        engine='python',
        sep=','
    )

    if save_file:
        file_name = datetime.now().strftime('DIVI_ITCU_%Y-%m-%d.csv')

        df.to_csv(
            PATH +
            file_name,
            sep=",",
            encoding='utf8',
            index=False
        )
    return df
