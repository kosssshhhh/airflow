from datetime import date, datetime
from typing import List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from enum import Enum


class Enum(str, Enum):
    ...


class TimestampMixin:
    @declared_attr
    def created_at(cls):
        return Column(DateTime, default=datetime.utcnow, nullable=False)

    @declared_attr
    def updated_at(cls):
        return Column(
            DateTime,
            default=datetime.utcnow,
            onupdate=datetime.utcnow,
            nullable=False,
        )


class CreateTimestampMixin:
    @declared_attr
    def created_at(cls):
        return Column(DateTime, default=datetime.utcnow, nullable=False)


class TrashTimestampMixin(TimestampMixin):
    @declared_attr
    def deleted_at(cls):
        return Column(DateTime, default=datetime.utcnow, nullable=False)
