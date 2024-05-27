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
from core.infra.database.models.base import Base


class Category(Base):
    __tablename__ = "category"
    __table_args__ = {"extend_existing": True}
    org_category_id = Column(String(255))
    category_id = Column(Integer, primary_key=True, autoincrement=True)
    mall_type = Column(Enum(MallType))
    name = Column(String(255))


class CategoryClosure(Base):
    __tablename__ = 'category_closure'
    __table_args__ = {"extend_existing": True}
    ancestor_id = Column(Integer, ForeignKey('category.category_id'), primary_key=True)
    descendant_id = Column(Integer, ForeignKey('category.category_id'), primary_key=True)
    mall_type = Column(Enum(MallType), primary_key=True)
    depth = Column(Integer)


class CategoryProduct(Base):
    __tablename__ = "category_product"
    __table_args__ = {"extend_existing": True}
    category_id = Column(Integer, ForeignKey("category.category_id"), primary_key=True)
    product_id = Column(String(255), ForeignKey("product.product_id"), primary_key=True)
    mall_type = Column(Enum(MallType), ForeignKey("product.mall_type"), primary_key=True)
    __table_args__ = (
        ForeignKeyConstraint(
            ['product_id', 'mall_type'],
            ['product.product_id', 'product.mall_type']
        ),
        {'extend_existing': True}
    )
    