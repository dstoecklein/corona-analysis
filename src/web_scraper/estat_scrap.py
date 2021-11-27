import eurostat
from datetime import datetime
from src.utils import paths

PATH_POP = paths.get_population_path()


def population_nuts_2(save_file: bool):
    df = eurostat.get_data_df(
        'demo_r_d2jan',
        flags=False
    )

    if save_file:
        file_name = datetime.now().strftime('ESTAST_POP_STATES_%Y-%m-%d.csv')

        df.to_csv(
            PATH_POP +
            file_name,
            sep=",",
            encoding='utf8',
            index=False
        )
    return df


def population_agegroups(save_file: bool):
    df = eurostat.get_data_df(
        'demo_pjangroup',
        flags=False
    ).copy()

    if save_file:
        file_name = datetime.now().strftime('ESTAST_POP_AGEGROUPS_%Y-%m-%d.csv')

        df.to_csv(
            PATH_POP +
            file_name,
            sep=",",
            encoding='utf8',
            index=False
        )
    return df
