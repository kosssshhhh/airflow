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
    mall_type = Column(Enum(MallType), primary_key=True)
    name = Column(String(255))


class CategoryClosure(Base):
    __tablename__ = 'category_closure'
    ancestor_id = Column(Integer, ForeignKey('category.category_id'), primary_key=True)
    descendant_id = Column(Integer, ForeignKey('category.category_id'), primary_key=True)
    mall_type = Column(Enum(MallType), ForeignKey('category.mall_type'), primary_key=True)
    depth = Column(Integer)

    # ForeignKey 설정은 각 id가 category 테이블의 category_id와 mall_type을 참조함을 명시
    # 예를 들어, ancestor_id 와 mall_type 컬럼의 조합이 category 테이블의 category_id와 mall_type을 참조
    __table_args__ = (
        ForeignKeyConstraint(
            ['ancestor_id', 'mall_type'],
            ['category.category_id', 'category.mall_type']),
        ForeignKeyConstraint(
            ['descendant_id', 'mall_type'],
            ['category.category_id', 'category.mall_type']),
    )

class CategoryProduct(Base):
    __tablename__ = "category_product"
    __table_args__ = {"extend_existing": True}
    category_id = Column(
        Integer, ForeignKey("category.category_id"), primary_key=True
    )
    product_id = Column(String(255), ForeignKey("product.product_id"), primary_key=True)
    mall_type = Column(Enum(MallType), ForeignKey("category.mall_type"), primary_key=True)
