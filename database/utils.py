from contextlib import contextmanager
from typing import Callable

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker


def get_managed_session_maker(SessionMaker: sessionmaker) -> Callable:
    @contextmanager
    def make_managed_session():
        session = SessionMaker()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise RuntimeError(e)
        except Exception as e:
            session.rollback()
            raise RuntimeError(e)
        finally:
            session.close()

    return make_managed_session


def create_sqlalchemy_engine(db_uri: str) -> Engine:
    return create_engine(db_uri)
