from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
    ForeignKeyConstraint,
    Enum
)
from sqlalchemy.orm import declarative_base
from core.infra.database.enum import MallType
from core.infra.database.models.base import MySQLBase


class Category(MySQLBase):
    __tablename__ = "category"
    __table_args__ = {"extend_existing": True}
    org_category_id = Column(String(255))
    category_id = Column(Integer, primary_key=True, autoincrement=True)
    mall_type_id = Column(Enum(MallType), ForeignKey("mall_type.mall_type_id"))
    name = Column(String(255))


class CategoryClosure(MySQLBase):
    __tablename__ = 'category_closure'
    __table_args__ = {"extend_existing": True}
    ancestor_id = Column(Integer, ForeignKey('category.category_id'), primary_key=True)
    descendant_id = Column(Integer, ForeignKey('category.category_id'), primary_key=True)
    mall_type_id = Column(Enum(MallType), ForeignKey("mall_type.mall_type_id"), primary_key=True)
    depth = Column(Integer)


class CategoryStyle(MySQLBase):
    __tablename__ = "category_style"
    __table_args__ = {"extend_existing": True} 
    category_id = Column(Integer, ForeignKey("category.category_id"), primary_key=True)
    style_id = Column(String(255), ForeignKey("style.style_id"), primary_key=True)
    mall_type_id = Column(Enum(MallType), ForeignKey("style.mall_type_id"), primary_key=True)
    __table_args__ = (
        ForeignKeyConstraint(
            ['style_id', 'mall_type_id'],
            ['style.style_id', 'style.mall_type_id']
        ),
        {'extend_existing': True}
    )
    