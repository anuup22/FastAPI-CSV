from sqlalchemy import Column, Integer, String
from .database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    firstName = Column(String, index=True)
    lastName = Column(String, index=True)
    age = Column(Integer)
    email = Column(String, unique=True, index=True)
    fileName = Column(String, index=True)