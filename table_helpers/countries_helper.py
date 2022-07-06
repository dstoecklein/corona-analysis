from typing import Any, Optional

from sqlalchemy.orm import Session

import database.tables as tbl


# TODO provide option to search for country_de, nuts_0, etc.
def get_country(session: Session, country_en: str) -> Optional[Any]:
    """
    Get a specific country

    Args:
        session: `Session` object from `sqlalchemy.orm`
        country_en: Country name in english

    Returns:
        A `row` object for the country or `None` if country not found.
    """
    country_row = (
        session.query(tbl.Countries.country_en).filter(
            tbl.Countries.country_en == country_en
        )
    ).one_or_none()
    return country_row


def add_new_country(session: Session, countries_dict: dict[str, dict[str, str]]) -> None:
    """
    Adds a new Country entry to the local SQLite database.

    Args:
        session: `Session` object from `sqlalchemy.orm`

        country_dict:
    """
    new_countries = list()

    for country_en, value_dict in countries_dict.items():

        # check if entry already exist
        country_exist = get_country(session=session, country_en=country_en)
        if country_exist is not None:
            continue

        # create new entry
        new_country = tbl.Countries(
            country_en=country_en,
            country_de=value_dict.get("country_de"),
            latitude=value_dict.get("latitude"),
            longitude=value_dict.get("longitude"),
            iso_3166_1_alpha2=value_dict.get("iso_3166_1_alpha2"),
            iso_3166_1_alpha3=value_dict.get("iso_3166_1_alpha3"),
            iso_3166_1_numeric=value_dict.get("iso_3166_1_numeric"),
            nuts_0=value_dict.get("nuts_0")
        )
        new_countries.append(new_country)

    session.add_all(new_countries)
