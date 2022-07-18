from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

import database.tables as tbl


def upsert_vaccine(session: Session, vaccines: list[dict[str, dict]]):
    for vaccine_dict in vaccines:
        for entry, values_dict in vaccine_dict.items():
            insert_stmt = insert(tbl.Vaccine).values(
                brand_name1=values_dict.get("brand_name1"),
                brand_name2=values_dict.get("brand_name2"),
                manufacturer=values_dict.get("manufacturer"),
                vaccine_type=values_dict.get("vaccine_type"),
            )
            do_update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["brand_name1"],
                set_=dict(
                    brand_name2=insert_stmt.excluded.brand_name2,
                    manufacturer=insert_stmt.excluded.manufacturer,
                    vaccine_type=insert_stmt.excluded.vaccine_type,
                ),
            )

            session.execute(do_update_stmt)


def upsert_vaccine_series(session: Session, vaccine_series: dict[int, str]):
    for series, description in vaccine_series.items():
        insert_stmt = insert(tbl.VaccineSeries).values(
            series=series,
            description=description,
        )
        do_update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["series"],
            set_=dict(
                description=insert_stmt.excluded.description,
            ),
        )

        session.execute(do_update_stmt)
