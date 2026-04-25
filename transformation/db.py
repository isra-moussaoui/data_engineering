from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from transformation.settings import get_postgres_settings


@lru_cache
def get_engine():
    return create_engine(get_postgres_settings().sqlalchemy_url, future=True)

def get_session():
    session_factory = sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, future=True)
    return session_factory()