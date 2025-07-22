from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.orm import class_mapper
import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Time, Float, Text, ForeignKey, JSON, Numeric, Date, \
    TIMESTAMP, UUID
from sqlalchemy.ext.declarative import declarative_base


@as_declarative()
class Base:
    id: int
    __name__: str

    # Auto-generate table name if not provided
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    # Generic to_dict() method
    def to_dict(self):
        """
        Converts the SQLAlchemy model instance to a dictionary, ensuring UUID fields are converted to strings.
        """
        result = {}
        for column in class_mapper(self.__class__).columns:
            value = getattr(self, column.key)
                # Handle UUID fields
            if isinstance(value, uuid.UUID):
                value = str(value)
            # Handle datetime fields
            elif isinstance(value, datetime):
                value = value.isoformat()  # Convert to ISO 8601 string
            # Handle Decimal fields
            elif isinstance(value, Decimal):
                value = float(value)

            result[column.key] = value
        return result




class Users(Base):
    __tablename__ = 'users'
    user_id = Column(Integer, primary_key=True)
    username = Column(String, primary_key=False)
    email = Column(String, primary_key=False)


class Posts(Base):
    __tablename__ = 'posts'
    post_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, primary_key=False)
    title = Column(String, primary_key=False)
    content = Column(String, primary_key=False)
    published_at = Column(Time, primary_key=False)


class Comments(Base):
    __tablename__ = 'comments'
    comment_id = Column(Integer, primary_key=True)
    post_id = Column(Integer, primary_key=False)
    user_id = Column(Integer, primary_key=False)
    comment_text = Column(String, primary_key=False)
    commented_at = Column(Time, primary_key=False)


class PostsSample(Base):
    __tablename__ = 'posts_sample'
    id = Column(Integer, primary_key=True)
    title = Column(String, primary_key=False)
    description = Column(String, primary_key=False)
    views = Column(Integer, primary_key=False)
    is_published = Column(Boolean, primary_key=False)
    created_at = Column(Time, primary_key=False)
    updated_at = Column(Time, primary_key=False)
    image = Column(LargeBinary, primary_key=False)
    reference_id = Column(UUID, primary_key=False)


class Test(Base):
    __tablename__ = 'test'
    id = Column(Integer, primary_key=True)
    title = Column(String, primary_key=False)
    description = Column(String, primary_key=False)
    views = Column(Integer, primary_key=False)
    rating = Column(Float, primary_key=False)
    is_published = Column(Boolean, primary_key=False)
    created_at = Column(Time, primary_key=False)
    updated_at = Column(Time, primary_key=False)
    image = Column(LargeBinary, primary_key=False)
    reference_id = Column(UUID, primary_key=False)


class MajorDataTypes(Base):
    __tablename__ = 'major_data_types'
    id = Column(Integer, primary_key=True)
    name = Column(String, primary_key=False)
    description = Column(String, primary_key=False)
    email = Column(String, primary_key=False)
    age = Column(Integer, primary_key=False)
    balance = Column(String, primary_key=False)
    rating = Column(Float, primary_key=False)
    is_active = Column(Boolean, primary_key=False)
    created_at = Column(Time, primary_key=False)
    updated_at = Column(Time, primary_key=False)
    metadata = Column(JSON, primary_key=False)
    tags = Column(String, primary_key=False)
    profile_picture = Column(LargeBinary, primary_key=False)
    user_uuid = Column(UUID, primary_key=False)


