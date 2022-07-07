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


# TODO provide option to search for country_de, nuts_0, etc.
def get_subdivision1(session: Session, nuts_1: str) -> Optional[Any]:
    """
    Get a specific Subdivision Level 1
    https://en.wikipedia.org/wiki/First-level_NUTS_of_the_European_Union

    Args:
        session: `Session` object from `sqlalchemy.orm`
        nuts_1: NUTS 1 code

    Returns:
        A `row` object for the Subdivision or `None` if not found.
    """
    subdiv1_row = (
        session.query(tbl.CountriesSubdivs1.nuts_1).filter(
            tbl.CountriesSubdivs1.nuts_1 == nuts_1
        )
    ).one_or_none()
    return subdiv1_row


def add_new_subdivision1(session: Session, subdivs1_dict: dict[str, dict[str, str]]) -> None:
    # TODO: change from nuts_1 to iso_3166_1 as key
    # TODO: Get rid of FK in dict, better read it via SQLAlchemy
    """
    Adds a new Subdivsion Level 1 entry to the local SQLite database.
    Please provide NUTS_1 code as key:
    https://en.wikipedia.org/wiki/First-level_NUTS_of_the_European_Union

    Args:
        session: `Session` object from `sqlalchemy.orm`

        `subdivs1_dict: {NUTS_1 : {countries_fk: value, ...}}`
    """
    new_subdivs1 = list()

    for nuts_1, value_dict in subdivs1_dict.items():

        # check if entry already exist
        subdiv1_exist = get_subdivision1(session=session, nuts_1=nuts_1)
        if subdiv1_exist is not None:
            continue

        # create new entry
        new_subdiv1 = tbl.CountriesSubdivs1(
            countries_fk=value_dict.get("countries_fk"),
            subdivision_1=value_dict.get("subdivision_1"),
            latitude=value_dict.get("latitude"),
            longitude=value_dict.get("longitude"),
            iso_3166_2=value_dict.get("iso_3166_2"),
            nuts_1=nuts_1,
            bundesland_id=value_dict.get("bundesland_id")
        )
        new_subdivs1.append(new_subdiv1)

    session.add_all(new_subdivs1)


# TODO provide option to search for country_de, nuts_0, etc.
def get_subdivision2(session: Session, nuts_2: str) -> Optional[Any]:
    """
    Get a specific Subdivision Level 2

    Args:
        session: `Session` object from `sqlalchemy.orm`
        nuts_2: NUTS 2 code

    Returns:
        A `row` object for the Subdivision or `None` if not found.
    """
    subdiv2_row = (
        session.query(tbl.CountriesSubdivs2.nuts_2).filter(
            tbl.CountriesSubdivs2.nuts_2 == nuts_2
        )
    ).one_or_none()
    return subdiv2_row


def add_new_subdivision2(session: Session, subdivs2_dict: dict[str, dict[str, str]]) -> None:
    # TODO: change from nuts_1 to iso_3166_1 as key
    # TODO: Get rid of FK in dict, better read it via SQLAlchemy
    """
    Adds a new Subdivsion Level 2 entry to the local SQLite database.
    Please provide NUTS_2 code as key:

    Args:
        session: `Session` object from `sqlalchemy.orm`

        `subdivs2_dict: {NUTS_2 : {country_subdivs_1_fk: value, ...}}`
    """
    new_subdivs2 = list()

    for nuts_2, value_dict in subdivs2_dict.items():

        # check if entry already exist
        subdiv2_exist = get_subdivision2(session=session, nuts_2=nuts_2)
        if subdiv2_exist is not None:
            continue

        # create new entry
        new_subdiv2 = tbl.CountriesSubdivs2(
            country_subdivs_1_fk=value_dict.get("country_subdivs_1_fk"),
            subdivision_2=value_dict.get("subdivision_2"),
            latitude=value_dict.get("latitude"),
            longitude=value_dict.get("longitude"),
            nuts_2=nuts_2,
        )
        new_subdivs2.append(new_subdiv2)

    session.add_all(new_subdivs2)


# TODO provide option to search for country_de, nuts_0, etc.
def get_subdivision3(session: Session, nuts_3: str) -> Optional[Any]:
    """
    Get a specific Subdivision Level 3

    Args:
        session: `Session` object from `sqlalchemy.orm`
        nuts_3: NUTS 3 code

    Returns:
        A `row` object for the Subdivision or `None` if not found.
    """
    subdiv3_row = (
        session.query(tbl.CountriesSubdivs3.nuts_3).filter(
            tbl.CountriesSubdivs3.nuts_3 == nuts_3
        )
    ).one_or_none()
    return subdiv3_row


def add_new_subdivision3(session: Session, subdivs3_dict: dict[str, dict[str, str]]) -> None:
    # TODO: change from nuts_1 to iso_3166_1 as key
    # TODO: Get rid of FK in dict, better read it via SQLAlchemy
    """
    Adds a new Subdivsion Level 3 entry to the local SQLite database.
    Please provide NUTS_3 code as key:

    Args:
        session: `Session` object from `sqlalchemy.orm`

        `subdivs3_dict: {NUTS_3 : {country_subdivs_2_fk: value, ...}}`
    """
    new_subdivs3 = list()

    for nuts_3, value_dict in subdivs3_dict.items():

        # check if entry already exist
        subdiv3_exist = get_subdivision3(session=session, nuts_3=nuts_3)
        if subdiv3_exist is not None:
            continue

        # create new entry
        new_subdiv3 = tbl.CountriesSubdivs3(
            country_subdivs_2_fk=value_dict.get("country_subdivs_2_fk"),
            subdivision_3=value_dict.get("subdivision_3"),
            latitude=value_dict.get("latitude"),
            longitude=value_dict.get("longitude"),
            nuts_3=nuts_3,
            ags=value_dict.get("ags")
        )
        new_subdivs3.append(new_subdiv3)

    session.add_all(new_subdivs3)
