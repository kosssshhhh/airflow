from airflow.dags.core.infra.database.models.review import ReviewProduct
from sqlalchemy import (
    Enum,
    Text,
    Date,
    Float,
    Column,
    ForeignKey,
    Integer,
    String,
    ForeignKeyConstraint,
)
from sqlalchemy.orm import declarative_base
from core.infra.database.enum import MallType
from core.infra.database.models.base import Base


class MusinsaVariable(Base):
    __tablename__ = "musinsa_variable"
    variable_id = Column(Integer, autoincrement=True, primary_key=True)
    product_id = Column(String(255), ForeignKey("product.product_id"))
    mall_type = Column(Enum(MallType), ForeignKey("product.mall_type"))
    product_num = Column(String(255))
    male_percentage = Column(Integer)
    female_percentage = Column(Integer)
    likes = Column(Integer)
    cumulative_sales = Column(Integer)
    age_under_18 = Column(Integer, name="under_18")
    age_19_to_23 = Column(Integer, name="19_to_23")
    age_24_to_28 = Column(Integer, name="24_to_28")
    age_29_to_33 = Column(Integer, name="29_to_33")
    age_34_to_39 = Column(Integer, name="34_to_39")
    age_over_40 = Column(Integer, name="over_40")
    __table_args__ = (
        ForeignKeyConstraint(
            ["product_id", "mall_type"], ["product.product_id", "product.mall_type"]
        ),
        {"extend_existing": True},
    )


class MusinsaReview(Base):
    __tablename__ = "musinsa_review"
    review_id = Column(Integer, ForeignKey('ReviewProduct.review_id') ,primary_key=True)
    org_review_id = Column(String(255), unique=True)
    product_id = Column(String(255))
    rate = Column(Integer)
    review_type = Column(String(255))
    user_info = Column(String(255))
    meta_data = Column(String(255))
    body = Column(Text)
    helpful = Column(Integer)
    good_style = Column(Integer)
    __table_args__ = (
        {"extend_existing": True},
    )
