from typing import Optional

from sqlalchemy.engine.row import Row
from sqlalchemy.orm import Session

import database.tables as tbl


def get_country(
    session: Session,
    country_en: str = None,
    country_de: str = None,
    iso_3166_1_alpha2: str = None,
    iso_3166_1_alpha3: str = None,
    iso_3166_1_numeric: int = None,
    nuts_0: str = None,
) -> Optional[tbl.Countries]:
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
        col = tbl.Countries.country_en
        col_value = country_en
    if country_de is not None:
        col = tbl.Countries.country_de
        col_value = country_de
    if iso_3166_1_alpha2 is not None:
        col = tbl.Countries.iso_3166_1_alpha2
        col_value = iso_3166_1_alpha2
    if iso_3166_1_alpha3 is not None:
        col = tbl.Countries.iso_3166_1_alpha3
        col_value = iso_3166_1_alpha3
    if iso_3166_1_numeric is not None:
        col = tbl.Countries.iso_3166_1_numeric
        col_value = iso_3166_1_numeric
    if nuts_0 is not None:
        col = tbl.Countries.nuts_0
        col_value = nuts_0

    # query
    row = (session.query(tbl.Countries).filter(col == col_value)).one_or_none()
    return row


# TODO: dict.get() return Optional[str], thus causing mypy error
# !/usr/bin/env python
# -*- coding: utf-8 -*-
# mypy: ignore-errors


def add_new_country(
    session: Session, countries_dict: dict[str, dict[str, str]]
) -> None:
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
            nuts_0=value_dict.get("nuts_0"),
        )
        new_countries.append(new_country)

    session.add_all(new_countries)
    session.flush()


def get_subdivision1(
    session: Session,
    subdivision_1: str = None,
    iso_3166_2: str = None,
    nuts_1: str = None,
    bundesland_id: int = None,
) -> Row:
    """
    Get a specific Subdivision Level 1
    https://en.wikipedia.org/wiki/First-level_NUTS_of_the_European_Union

    Args:
        session: `Session` object from `sqlalchemy.orm`
        subdivision_1: Name of the subdivision in the countries language
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
            subdivision_1,
            iso_3166_2,
            nuts_1,
            bundesland_id,
        ]
    ):
        raise RuntimeError(
            "Please provide either the subdiv. name, iso_3166, NUTS code or Bundesland ID"
        )

    if subdivision_1 is not None:
        col = tbl.CountriesSubdivs1.subdivision_1
        col_value = subdivision_1
    if iso_3166_2 is not None:
        col = tbl.CountriesSubdivs1.iso_3166_2
        col_value = iso_3166_2
    if nuts_1 is not None:
        col = tbl.CountriesSubdivs1.nuts_1
        col_value = nuts_1
    if bundesland_id is not None:
        col = tbl.CountriesSubdivs1.bundesland_id
        col_value = bundesland_id

    # query
    row = (
        (
            session.query(
                tbl.CountriesSubdivs1.subdivision_1,
                tbl.CountriesSubdivs1.latitude.label("subdiv1_latitude"),
                tbl.CountriesSubdivs1.longitude.label("subdiv1_longitude"),
                tbl.CountriesSubdivs1.iso_3166_2,
                tbl.CountriesSubdivs1.nuts_1,
                tbl.Countries.country_en,
                tbl.Countries.country_de,
                tbl.Countries.latitude.label("country_latitude"),
                tbl.Countries.longitude.label("country_longitude"),
                tbl.Countries.iso_3166_1_alpha2,
                tbl.Countries.iso_3166_1_alpha3,
                tbl.Countries.iso_3166_1_numeric,
                tbl.Countries.nuts_0,
            )
            .join(tbl.Countries)
            .filter(col == col_value)
        )
        .one_or_none()
        #._mapping
    )

    return row


def add_new_subdivision1(
    session: Session, subdivs1_dict: dict[str, dict[str, str]]
) -> None:
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
            bundesland_id=value_dict.get("bundesland_id"),
        )
        new_subdivs1.append(new_subdiv1)

    session.add_all(new_subdivs1)
    session.flush()

def get_subdivision2(
    session: Session, subdivision_2: str = None, nuts_2: str = None
) -> Row:
    """
    Get a specific Subdivision Level 2

    Args:
        session: `Session` object from `sqlalchemy.orm`
        subdivision_2: Name of the subdivision in the countries language
        nuts_2: NUTS Level 2 code

    Returns:
        A `row` object of the Subdivision or `None` if not found.
    """

    # check if all parameters are None
    if all(v is None for v in [subdivision_2, nuts_2]):
        raise RuntimeError(
            "Please provide either the subdiv. name, iso_3166, NUTS code or Bundesland ID"
        )

    if subdivision_2 is not None:
        col = tbl.CountriesSubdivs2.subdivision_2
        col_value = subdivision_2
    if nuts_2 is not None:
        col = tbl.CountriesSubdivs2.nuts_2
        col_value = nuts_2

    # query
    row = (
        (
            session.query(
                tbl.CountriesSubdivs2.subdivision_2,
                tbl.CountriesSubdivs2.latitude.label("subdiv2_latitude"),
                tbl.CountriesSubdivs2.longitude.label("subdiv2_longitude"),
                tbl.CountriesSubdivs2.nuts_2,
                tbl.CountriesSubdivs1.subdivision_1,
                tbl.CountriesSubdivs1.latitude.label("subdiv1_latitude"),
                tbl.CountriesSubdivs1.longitude.label("subdiv1_longitude"),
                tbl.CountriesSubdivs1.iso_3166_2,
                tbl.CountriesSubdivs1.nuts_1,
                tbl.Countries.country_en,
                tbl.Countries.country_de,
                tbl.Countries.latitude.label("country_latitude"),
                tbl.Countries.longitude.label("country_longitude"),
                tbl.Countries.iso_3166_1_alpha2,
                tbl.Countries.iso_3166_1_alpha3,
                tbl.Countries.iso_3166_1_numeric,
                tbl.Countries.nuts_0,
            )
            .join(
                tbl.CountriesSubdivs1,
                tbl.CountriesSubdivs2.country_subdivs_1_fk
                == tbl.CountriesSubdivs1.country_subdivs_1_id,
            )
            .join(
                tbl.Countries,
                tbl.CountriesSubdivs1.countries_fk == tbl.Countries.countries_id,
            )
            .filter(col == col_value)
        )
        .one_or_none()
        #._mapping
    )
    return row


def add_new_subdivision2(
    session: Session, subdivs2_dict: dict[str, dict[str, str]]
) -> None:
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
    session.flush()

def get_subdivision3(
    session: Session, subdivision_3: str = None, nuts_3: str = None, ags: int = None
) -> Row:
    """
    Get a specific Subdivision Level 3

    Args:
        session: `Session` object from `sqlalchemy.orm`
        subdivision_3: Name of the subdivision in the countries language
        nuts_3: NUTS Level 3 code
        ags: !Germany only! Amtlicher Gemeindeschluessel

    Returns:
        A `row` object for the Subdivision or `None` if not found.
    """

    # check if all parameters are None
    if all(v is None for v in [subdivision_3, nuts_3, ags]):
        raise RuntimeError(
            "Please provide either the subdiv. name, iso_3166, NUTS code or Bundesland ID"
        )

    if subdivision_3 is not None:
        col = tbl.CountriesSubdivs3.subdivision_3
        col_value = subdivision_3
    if nuts_3 is not None:
        col = tbl.CountriesSubdivs3.nuts_3
        col_value = nuts_3
    if ags is not None:
        col = tbl.CountriesSubdivs3.ags
        col_value = ags

    # query
    row = (
        (
            session.query(
                tbl.CountriesSubdivs3.subdivision_3,
                tbl.CountriesSubdivs3.latitude.label("subdiv3_latitude"),
                tbl.CountriesSubdivs3.longitude.label("subdiv3_longitude"),
                tbl.CountriesSubdivs3.nuts_3,
                tbl.CountriesSubdivs3.ags,
                tbl.CountriesSubdivs2.subdivision_2,
                tbl.CountriesSubdivs2.latitude.label("subdiv2_latitude"),
                tbl.CountriesSubdivs2.longitude.label("subdiv2_longitude"),
                tbl.CountriesSubdivs2.nuts_2,
                tbl.CountriesSubdivs1.subdivision_1,
                tbl.CountriesSubdivs1.latitude.label("subdiv1_latitude"),
                tbl.CountriesSubdivs1.longitude.label("subdiv1_longitude"),
                tbl.CountriesSubdivs1.iso_3166_2,
                tbl.CountriesSubdivs1.nuts_1,
                tbl.Countries.country_en,
                tbl.Countries.country_de,
                tbl.Countries.latitude.label("country_latitude"),
                tbl.Countries.longitude.label("country_longitude"),
                tbl.Countries.iso_3166_1_alpha2,
                tbl.Countries.iso_3166_1_alpha3,
                tbl.Countries.iso_3166_1_numeric,
                tbl.Countries.nuts_0,
            )
            .join(
                tbl.CountriesSubdivs2,
                tbl.CountriesSubdivs3.country_subdivs_2_fk
                == tbl.CountriesSubdivs2.country_subdivs_2_id,
            )
            .join(
                tbl.CountriesSubdivs1,
                tbl.CountriesSubdivs2.country_subdivs_1_fk
                == tbl.CountriesSubdivs1.country_subdivs_1_id,
            )
            .join(
                tbl.Countries,
                tbl.CountriesSubdivs1.countries_fk == tbl.Countries.countries_id,
            )
            .filter(col == col_value)
        )
        .one_or_none()
        #._mapping
    )

    return row


def add_new_subdivision3(
    session: Session, subdivs3_dict: dict[str, dict[str, str]]
) -> None:
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
            ags=value_dict.get("ags"),
        )
        new_subdivs3.append(new_subdiv3)

    session.add_all(new_subdivs3)
    session.flush()