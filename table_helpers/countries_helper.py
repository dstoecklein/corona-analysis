import pandas as pd
from typing import Optional

from sqlalchemy.engine.row import Row
from sqlalchemy.orm import Session

import database.tables as tbl

def get_countries_with_subdivisions_df(session: Session) -> pd.DataFrame:
    """
    Get all countries with all subdivisions as Pandas DataFrame

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        Pandas DataFrame
       
    """
    countries_df = pd.read_sql(
        session.query(
            tbl.Country.country_id,
            tbl.Country.country_en,
            tbl.Country.country_de,
            tbl.Country.latitude.label("country_latitude"),
            tbl.Country.longitude.label("country_longitude"),
            tbl.Country.iso_3166_1_alpha2,
            tbl.Country.iso_3166_1_alpha3,
            tbl.Country.nuts_0,
            tbl.CountrySubdivision1.country_subdivision1_id,
            tbl.CountrySubdivision1.country_fk,
            tbl.CountrySubdivision1.subdivision1,
            tbl.CountrySubdivision1.latitude.label("subdivision1_latitude"),
            tbl.CountrySubdivision1.longitude.label("subdivision1_longitude"),
            tbl.CountrySubdivision1.iso_3166_2,
            tbl.CountrySubdivision1.bundesland_id,
            tbl.CountrySubdivision2.country_subdivision2_id,
            tbl.CountrySubdivision2.country_subdivision1_fk,
            tbl.CountrySubdivision2.subdivision2,
            tbl.CountrySubdivision2.latitude.label("subdivision2_latitude"),
            tbl.CountrySubdivision2.longitude.label("subdivision2_longitude"),
            tbl.CountrySubdivision2.nuts_2,
            tbl.CountrySubdivision3.country_subdivision3_id,
            tbl.CountrySubdivision3.country_subdivision2_fk,
            tbl.CountrySubdivision3.subdivision3,
            tbl.CountrySubdivision3.latitude.label("subdivision3_latitude"),
            tbl.CountrySubdivision3.longitude.label("subdivision3_longitude"),
            tbl.CountrySubdivision3.nuts_3,
            tbl.CountrySubdivision3.ags,
        )
        .join(
            tbl.CountrySubdivision1,
            tbl.Country.country_id == tbl.CountrySubdivision1.country_fk,
        )
        .join(
            tbl.CountrySubdivision2,
            tbl.CountrySubdivision1.country_subdivision1_id == tbl.CountrySubdivision2.country_subdivision1_fk,
        )
        .join(
            tbl.CountrySubdivision3,
            tbl.CountrySubdivision2.country_subdivision2_id == tbl.CountrySubdivision3.country_subdivision2_fk,
        )
        .order_by(tbl.Country.country_id)
        .statement, 
        session.bind
    )
    return countries_df

def get_countries(session: Session) -> list[tbl.Country]:
    """
    Get all countries in database

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
       `list` of `Country` objects
    """
    countries = (
        session.query(tbl.Country).order_by(tbl.Country.country_id)
    ).all()
    return countries
   
def get_countries_df(session: Session) -> pd.DataFrame:
    """
    Get all countries in database as Pandas DataFrame

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        Pandas DataFrame
       
    """
    countries_df = pd.read_sql(
        session.query(tbl.Country)
        .order_by(tbl.Country.country_id)
        .statement, 
        session.bind
    )
    return countries_df

def get_country(
    session: Session,
    country_en: str = None,
    country_de: str = None,
    iso_3166_1_alpha2: str = None,
    iso_3166_1_alpha3: str = None,
    iso_3166_1_numeric: int = None,
    nuts_0: str = None,
) -> Optional[tbl.Country]:
    """
    Get a specific country

    Args:
        session: `Session` object from `sqlalchemy.orm`
        country_en: Country name in english
        country_de: Country name in german
        iso_3166_1_alpha2: ISO 3166 code alphanumeric 2 digits
        iso_3166_1_alpha3: ISO 3166 code alphanumeric 3 digits
        iso_3166_numeric: ISO 3166 numeric
        nuts_0: NUTS level 0 code

    Returns:
        A `row` object of the country or `None` if country not found.
    """

    # check if all parameters are None
    if all(
        v is None
        for v in [
            country_en,
            country_de,
            iso_3166_1_alpha2,
            iso_3166_1_alpha3,
            iso_3166_1_numeric,
            nuts_0,
        ]
    ):
        raise RuntimeError(
            "Please provide either the country name, iso_3166 code or NUTS code"
        )

    if country_en is not None:
        col = tbl.Country.country_en
        col_value = country_en
    if country_de is not None:
        col = tbl.Country.country_de
        col_value = country_de
    if iso_3166_1_alpha2 is not None:
        col = tbl.Country.iso_3166_1_alpha2
        col_value = iso_3166_1_alpha2
    if iso_3166_1_alpha3 is not None:
        col = tbl.Country.iso_3166_1_alpha3
        col_value = iso_3166_1_alpha3
    if iso_3166_1_numeric is not None:
        col = tbl.Country.iso_3166_1_numeric
        col_value = iso_3166_1_numeric
    if nuts_0 is not None:
        col = tbl.Country.nuts_0
        col_value = nuts_0

    # query
    row = (session.query(tbl.Country).filter(col == col_value)).one_or_none()
    return row


# dict.get() return Optional[str], thus causing mypy error
# !/usr/bin/env python
# -*- coding: utf-8 -*-
# mypy: ignore-errors


def insert_country(session: Session, countries_dict: dict[str, dict[str, str]]) -> None:
    """
    Adds a new Country entry to the local SQLite database.

    Args:
        session: `Session` object from `sqlalchemy.orm`

        country_dict:
    """
    new_entries = list()

    for country_en, values_dict in countries_dict.items():

        # check if entry already exist
        country_exist = get_country(session=session, country_en=country_en)
        if country_exist is not None:
            continue

        # create new entry
        entry = tbl.Country(
            country_en=country_en,
            country_de=values_dict.get("country_de"),
            latitude=values_dict.get("latitude"),
            longitude=values_dict.get("longitude"),
            iso_3166_1_alpha2=values_dict.get("iso_3166_1_alpha2"),
            iso_3166_1_alpha3=values_dict.get("iso_3166_1_alpha3"),
            iso_3166_1_numeric=values_dict.get("iso_3166_1_numeric"),
            nuts_0=values_dict.get("nuts_0"),
        )
        new_entries.append(entry)

    session.add_all(new_entries)
    session.flush()


def get_subdivisions1(session: Session) -> list[tbl.CountrySubdivision1]:
    """
    Get all Subdivision of Level 1 in database

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
       `list` of `CountrySubdivision1` objects
    """
    subdivisions1 = (
        session.query(tbl.CountrySubdivision1)
        .order_by(tbl.CountrySubdivision1.country_subdivision1_id)
    ).all()
    return subdivisions1
   
def get_subdivisions1_df(session: Session) -> pd.DataFrame:
    """
    Get all Subdivision of Level 1 in database as Pandas DataFrame

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        Pandas DataFrame
       
    """
    subdivisions1_df = pd.read_sql(
        session.query(tbl.CountrySubdivision1)
        .order_by(tbl.CountrySubdivision1.country_subdivision1_id)
        .statement, 
        session.bind
    )
    return subdivisions1_df

def get_subdivision1(
    session: Session,
    subdivision1: str = None,
    iso_3166_2: str = None,
    nuts_1: str = None,
    bundesland_id: int = None,
) -> Row:
    """
    Get a specific Subdivision Level 1
    https://en.wikipedia.org/wiki/First-level_NUTS_of_the_European_Union

    Args:
        session: `Session` object from `sqlalchemy.orm`
        subdivision1: Name of the subdivision in the countries language
        iso_3166_2: ISO 3166 code alphanumeric
        nuts_1: NUTS Level 1 code
        bundesland_id: ID of German Bundesland

    Returns:
        A `row` object of the Subdivision or `None` if not found.
    """

    # check if all parameters are None
    if all(
        v is None
        for v in [
            subdivision1,
            iso_3166_2,
            nuts_1,
            bundesland_id,
        ]
    ):
        raise RuntimeError(
            "Please provide either the subdiv. name, iso_3166, NUTS code or Bundesland ID"
        )

    if subdivision1 is not None:
        col = tbl.CountrySubdivision1.subdivision1
        col_value = subdivision1
    if iso_3166_2 is not None:
        col = tbl.CountrySubdivision1.iso_3166_2
        col_value = iso_3166_2
    if nuts_1 is not None:
        col = tbl.CountrySubdivision1.nuts_1
        col_value = nuts_1
    if bundesland_id is not None:
        col = tbl.CountrySubdivision1.bundesland_id
        col_value = bundesland_id

    # query
    row = (
        session.query(
            tbl.CountrySubdivision1.subdivision1,
            tbl.CountrySubdivision1.latitude.label("subdivision1_latitude"),
            tbl.CountrySubdivision1.longitude.label("subdivision1_longitude"),
            tbl.CountrySubdivision1.iso_3166_2,
            tbl.CountrySubdivision1.nuts_1,
            tbl.Country.country_en,
            tbl.Country.country_de,
            tbl.Country.latitude.label("country_latitude"),
            tbl.Country.longitude.label("country_longitude"),
            tbl.Country.iso_3166_1_alpha2,
            tbl.Country.iso_3166_1_alpha3,
            tbl.Country.iso_3166_1_numeric,
            tbl.Country.nuts_0,
        )
        .join(tbl.Country)
        .filter(col == col_value)
    ).one_or_none()

    return row


def insert_subdivision1(
    session: Session, subdivisions1_dict: dict[str, dict[str, str]]
) -> None:
    # TODO: change from nuts_1 to iso_3166_1 as key
    # TODO: Get rid of FK in dict, better read it via SQLAlchemy
    """
    Adds a new Subdivision Level 1 entry to the local SQLite database.
    Please provide NUTS_1 code as key:
    https://en.wikipedia.org/wiki/First-level_NUTS_of_the_European_Union

    Args:
        session: `Session` object from `sqlalchemy.orm`

        `subdivisions1_dict: {NUTS_1 : {country_fk: value, ...}}`
    """
    new_entries = list()

    for nuts_1, values_dict in subdivisions1_dict.items():

        # check if entry already exist
        subdivision1_exist = get_subdivision1(session=session, nuts_1=nuts_1)

        if subdivision1_exist is not None:
            continue

        # create new entry
        entry = tbl.CountrySubdivision1(
            country_fk=values_dict.get("country_fk"),
            subdivision1=values_dict.get("subdivision1"),
            latitude=values_dict.get("latitude"),
            longitude=values_dict.get("longitude"),
            iso_3166_2=values_dict.get("iso_3166_2"),
            nuts_1=nuts_1,
            bundesland_id=values_dict.get("bundesland_id"),
        )
        new_entries.append(entry)

    session.add_all(new_entries)
    session.flush()

def get_subdivisions2(session: Session) -> list[tbl.CountrySubdivision2]:
    """
    Get all Subdivision of Level 2 in database

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
       `list` of `CountrySubdivision2` objects
    """
    subdivisions2 = (
        session.query(tbl.CountrySubdivision2)
        .order_by(tbl.CountrySubdivision2.country_subdivision2_id)
    ).all()
    return subdivisions2
   
def get_subdivisions2_df(session: Session) -> pd.DataFrame:
    """
    Get all Subdivision of Level 2 in database as Pandas DataFrame

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        Pandas DataFrame
       
    """
    subdivisions2_df = pd.read_sql(
        session.query(tbl.CountrySubdivision2)
        .order_by(tbl.CountrySubdivision2.country_subdivision2_id)
        .statement, 
        session.bind
    )
    return subdivisions2_df

def get_subdivision2(
    session: Session, subdivision2: str = None, nuts_2: str = None
) -> Row:
    """
    Get a specific Subdivision Level 2

    Args:
        session: `Session` object from `sqlalchemy.orm`
        subdivision2: Name of the subdivision in the countries language
        nuts_2: NUTS Level 2 code

    Returns:
        A `row` object of the Subdivision or `None` if not found.
    """

    # check if all parameters are None
    if all(v is None for v in [subdivision2, nuts_2]):
        raise RuntimeError(
            "Please provide either the subdiv. name, iso_3166, NUTS code or Bundesland ID"
        )

    if subdivision2 is not None:
        col = tbl.CountrySubdivision2.subdivision2
        col_value = subdivision2
    if nuts_2 is not None:
        col = tbl.CountrySubdivision2.nuts_2
        col_value = nuts_2

    # query
    row = (
        session.query(
            tbl.CountrySubdivision2.subdivision2,
            tbl.CountrySubdivision2.latitude.label("subdivision2_latitude"),
            tbl.CountrySubdivision2.longitude.label("subdivision2_longitude"),
            tbl.CountrySubdivision2.nuts_2,
            tbl.CountrySubdivision1.subdivision1,
            tbl.CountrySubdivision1.latitude.label("subdivision1_latitude"),
            tbl.CountrySubdivision1.longitude.label("subdivision1_longitude"),
            tbl.CountrySubdivision1.iso_3166_2,
            tbl.CountrySubdivision1.nuts_1,
            tbl.Country.country_en,
            tbl.Country.country_de,
            tbl.Country.latitude.label("country_latitude"),
            tbl.Country.longitude.label("country_longitude"),
            tbl.Country.iso_3166_1_alpha2,
            tbl.Country.iso_3166_1_alpha3,
            tbl.Country.iso_3166_1_numeric,
            tbl.Country.nuts_0,
        )
        .join(
            tbl.CountrySubdivision1,
            tbl.CountrySubdivision2.country_subdivision1_fk
            == tbl.CountrySubdivision1.country_subdivision1_id,
        )
        .join(
            tbl.Country,
            tbl.CountrySubdivision1.country_fk == tbl.Country.country_id,
        )
        .filter(col == col_value)
    ).one_or_none()
    return row


def insert_subdivision2(
    session: Session, subdivisions2_dict: dict[str, dict[str, str]]
) -> None:
    # TODO: change from nuts_1 to iso_3166_1 as key
    # TODO: Get rid of FK in dict, better read it via SQLAlchemy
    """
    Adds a new Subdivision Level 2 entry to the local SQLite database.
    Please provide NUTS_2 code as key:

    Args:
        session: `Session` object from `sqlalchemy.orm`

        `subdivisions2_dict: {NUTS_2 : {country_subdivision1_fk: value, ...}}`
    """
    new_entries = list()

    for nuts_2, values_dict in subdivisions2_dict.items():

        # check if entry already exist
        subdivision2_exist = get_subdivision2(session=session, nuts_2=nuts_2)
        if subdivision2_exist is not None:
            continue

        # create new entry
        entry = tbl.CountrySubdivision2(
            country_subdivision1_fk=values_dict.get("country_subdivision1_fk"),
            subdivision2=values_dict.get("subdivision2"),
            latitude=values_dict.get("latitude"),
            longitude=values_dict.get("longitude"),
            nuts_2=nuts_2,
        )
        new_entries.append(entry)

    session.add_all(new_entries)
    session.flush()

def get_subdivisions3(session: Session) -> list[tbl.CountrySubdivision3]:
    """
    Get all Subdivision of Level 3 in database

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
       `list` of `CountrySubdivision3` objects
    """
    subdivisions3 = (
        session.query(tbl.CountrySubdivision3)
        .order_by(tbl.CountrySubdivision3.country_subdivision3_id)
    ).all()
    return subdivisions3
   
def get_subdivisions3_df(session: Session) -> pd.DataFrame:
    """
    Get all Subdivision of Level 3 in database as Pandas DataFrame

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        Pandas DataFrame
       
    """
    subdivisions3_df = pd.read_sql(
        session.query(tbl.CountrySubdivision3)
        .order_by(tbl.CountrySubdivision3.country_subdivision3_id)
        .statement, 
        session.bind
    )
    return subdivisions3_df

def get_subdivision3(
    session: Session, subdivision3: str = None, nuts_3: str = None, ags: int = None
) -> Row:
    """
    Get a specific Subdivision Level 3

    Args:
        session: `Session` object from `sqlalchemy.orm`
        subdivision3: Name of the subdivision in the countries language
        nuts_3: NUTS Level 3 code
        ags: !Germany only! Amtlicher Gemeindeschluessel

    Returns:
        A `row` object for the Subdivision or `None` if not found.
    """

    # check if all parameters are None
    if all(v is None for v in [subdivision3, nuts_3, ags]):
        raise RuntimeError(
            "Please provide either the subdiv. name, iso_3166, NUTS code or Bundesland ID"
        )

    if subdivision3 is not None:
        col = tbl.CountrySubdivision3.subdivision3
        col_value = subdivision3
    if nuts_3 is not None:
        col = tbl.CountrySubdivision3.nuts_3
        col_value = nuts_3
    if ags is not None:
        col = tbl.CountrySubdivision3.ags
        col_value = ags

    # query
    row = (
        session.query(
            tbl.CountrySubdivision3.subdivision3,
            tbl.CountrySubdivision3.latitude.label("subdivision3_latitude"),
            tbl.CountrySubdivision3.longitude.label("subdivision3_longitude"),
            tbl.CountrySubdivision3.nuts_3,
            tbl.CountrySubdivision3.ags,
            tbl.CountrySubdivision2.subdivision2,
            tbl.CountrySubdivision2.latitude.label("subdivision2_latitude"),
            tbl.CountrySubdivision2.longitude.label("subdivision2_longitude"),
            tbl.CountrySubdivision2.nuts_2,
            tbl.CountrySubdivision1.subdivision1,
            tbl.CountrySubdivision1.latitude.label("subdivision1_latitude"),
            tbl.CountrySubdivision1.longitude.label("subdivision1_longitude"),
            tbl.CountrySubdivision1.iso_3166_2,
            tbl.CountrySubdivision1.nuts_1,
            tbl.Country.country_en,
            tbl.Country.country_de,
            tbl.Country.latitude.label("country_latitude"),
            tbl.Country.longitude.label("country_longitude"),
            tbl.Country.iso_3166_1_alpha2,
            tbl.Country.iso_3166_1_alpha3,
            tbl.Country.iso_3166_1_numeric,
            tbl.Country.nuts_0,
        )
        .join(
            tbl.CountrySubdivision2,
            tbl.CountrySubdivision3.country_subdivision2_fk
            == tbl.CountrySubdivision2.country_subdivision2_id,
        )
        .join(
            tbl.CountrySubdivision1,
            tbl.CountrySubdivision2.country_subdivision1_fk
            == tbl.CountrySubdivision1.country_subdivision1_id,
        )
        .join(
            tbl.Country,
            tbl.CountrySubdivision1.country_fk == tbl.Country.country_id,
        )
        .filter(col == col_value)
    ).one_or_none()

    return row


def insert_subdivision3(
    session: Session, subdivisions3_dict: dict[str, dict[str, str]]
) -> None:
    # TODO: change from nuts_1 to iso_3166_1 as key
    # TODO: Get rid of FK in dict, better read it via SQLAlchemy
    """
    Adds a new Subdivision Level 3 entry to the local SQLite database.
    Please provide NUTS_3 code as key:

    Args:
        session: `Session` object from `sqlalchemy.orm`

        `subdivisions3_dict: {NUTS_3 : {country_subdivision2_fk: value, ...}}`
    """
    new_entries = list()

    for nuts_3, values_dict in subdivisions3_dict.items():

        # check if entry already exist
        subdivision3_exist = get_subdivision3(session=session, nuts_3=nuts_3)
        if subdivision3_exist is not None:
            continue

        # create new entry
        entry = tbl.CountrySubdivision3(
            country_subdivision2_fk=values_dict.get("country_subdivision2_fk"),
            subdivision3=values_dict.get("subdivision3"),
            latitude=values_dict.get("latitude"),
            longitude=values_dict.get("longitude"),
            nuts_3=nuts_3,
            ags=values_dict.get("ags"),
        )
        new_entries.append(entry)

    session.add_all(new_entries)
    session.flush()
