from sqlalchemy import Enum, Date, Float, Column, ForeignKey, Integer, String, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.orm import declarative_base
from core.infra.database.enum import MallType
from core.infra.database.models.base import MySQLBase

class Style(MySQLBase):
    __tablename__ = "style"
    __table_args__ = {"extend_existing": True}
    style_id = Column(String(255), primary_key=True)
    mall_type_id = Column(Enum(MallType), ForeignKey("mall_type.mall_type_id"), primary_key=True)



class StyleRanking(MySQLBase):
    __tablename__ = "style_ranking"
    rank_id = Column(Integer, autoincrement=True, primary_key=True)
    category_id = Column(Integer, ForeignKey("category_style.category_id"))
    style_id = Column(String(255), ForeignKey("category_style.style_id"))
    style_name = Column(String(255))
    mall_type_id = Column(Enum(MallType), ForeignKey("category_style.mall_type_id"))
    rank_score = Column(Float)
    fixed_price = Column(Integer)
    brand = Column(String(255))
    discounted_price = Column(Integer)
    monetary_unit = Column(String(255))
    crawled_date = Column(Date)
    __table_args__ = (
        UniqueConstraint('category_id', 'style_id', 'mall_type_id', 'brand','rank_score', 'crawled_date', name='unique_style_ranking'),
        ForeignKeyConstraint(
            ['style_id', 'category_id', 'mall_type_id'],
            ['category_style.style_id', 'category_style.category_id', 'category_style.mall_type_id']
        ),
        {"extend_existing": True}
    )

class SKUAttribute(MySQLBase):
    __tablename__ = "sku_attribute"
    sku_id = Column(Integer, autoincrement= True, primary_key= True)
    style_id = Column(String(255), ForeignKey("style.style_id"))
    mall_type_id = Column(String(255), ForeignKey("style.mall_type_id"))
    attr_key = Column(String(255))
    attr_value = Column(String(255))
    __table_args__ = (
        ForeignKeyConstraint(
            ["style_id", "mall_type_id"], ["style.style_id", "style.mall_type_id"]
        ),
        {"extend_existing": True}
    )

class MallType(MySQLBase):
    __tablename__ = "mall_type"
    mall_type_id = Column(String(255), primary_key=True)
    mall_type_name = Column(String(255))
    