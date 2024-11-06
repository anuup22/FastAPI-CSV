from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Import all models here for Alembic
from ..models.user import User  # noqa