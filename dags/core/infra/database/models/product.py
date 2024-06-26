from sqlalchemy import Enum, Date, Float, Column, ForeignKey, Integer, String, ForeignKeyConstraint
from sqlalchemy.orm import declarative_base
from core.infra.database.enum import MallType
from core.infra.database.models.base import Base

class Product(Base):
    __tablename__ = "product"
    __table_args__ = {"extend_existing": True}
    product_id = Column(String(255), primary_key=True)
    mall_type = Column(Enum(MallType), primary_key=True)



class ProductRanking(Base):
    __tablename__ = "product_ranking"
    rank_id = Column(Integer, autoincrement=True, primary_key=True)
    category_id = Column(Integer, ForeignKey("category_product.category_id"))
    product_id = Column(String(255), ForeignKey("category_product.product_id"))
    mall_type = Column(Enum(MallType), ForeignKey("category_product.mall_type"))
    rank_score = Column(Float)
    fixed_price = Column(Integer)
    brand = Column(String(255))
    discounted_price = Column(Integer)
    monetary_unit = Column(String(255))
    crawled_date = Column(Date)
    __table_args__ = (
        ForeignKeyConstraint(
            ['product_id', 'category_id', 'mall_type'],
            ['category_product.product_id', 'category_product.category_id', 'category_product.mall_type']
        ),
        {"extend_existing": True}
    )

class SKUAttribute(Base):
    __tablename__ = "sku_attribute"
    sku_id = Column(Integer, autoincrement= True, primary_key= True)
    product_id = Column(String(255), ForeignKey("product.product_id"))
    mall_type = Column(Enum(MallType), ForeignKey("product.mall_type"))
    attr_key = Column(String(255))
    attr_value = Column(String(255))
    __table_args__ = (
        ForeignKeyConstraint(
            ["product_id", "mall_type"], ["product.product_id", "product.mall_type"]
        ),
        {"extend_existing": True}
    )
