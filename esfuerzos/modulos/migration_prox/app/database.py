from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.pool import StaticPool
from app.config import get_settings

settings = get_settings()

_is_sqlite = settings.database_url.startswith("sqlite")

engine = create_engine(
    settings.database_url,
    **(
        {
            "connect_args": {"check_same_thread": False},
            "poolclass": StaticPool,
        }
        if _is_sqlite
        else {
            "pool_pre_ping": True,
            "pool_size": 5,
            "max_overflow": 10,
        }
    ),
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
