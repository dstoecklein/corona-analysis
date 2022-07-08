from typing import Any, Optional

from sqlalchemy.orm import Session

import database.tables as tbl


def get_icd10(session: Session, icd10: str) -> Optional[Any]:
    """
    Get a specific icd10 code

    Args:
        session: `Session` object from `sqlalchemy.orm`
        agegroup: ICD10 code

    Returns:
        A `row` object for the ICD10 or `None` if ICD10 not found.
    """
    icd10_row = (
        session.query(tbl.ClassificationICD10.icd10).filter(
            tbl.ClassificationICD10.icd10 == icd10
        )
    ).one_or_none()
    return icd10_row


def insert_icd10(session: Session, icd10_dict: dict[str, dict[str, str]]) -> None:
    """
    Adds a new ICD10 entry to the local SQLite database.

    Args:
        session: `Session` object from `sqlalchemy.orm`

        icd10: Expected format is `dict{icd10:{description_en: value, description_de: value},...}`
    """
    new_icd10s = list()

    for icd10, value_dict in icd10_dict.items():

        # check if entry already exist
        icd10_exist = get_icd10(session=session, icd10=icd10)
        if icd10_exist is not None:
            continue

        # create new entry
        entry = tbl.ClassificationICD10(
            icd10=icd10,
            description_en=value_dict.get("description_en"),
            description_de=value_dict.get("description_de"),
        )
        new_icd10s.append(entry)

    session.add_all(new_icd10s)
