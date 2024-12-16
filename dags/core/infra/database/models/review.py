from sqlalchemy import Enum, Text, Date, Float, Column, ForeignKey, Integer, String, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.orm import declarative_base
from core.infra.database.enum import MallType
from core.infra.database.models.base import MySQLBase

class ReviewStyle(MySQLBase):
    __tablename__ = 'review_style'
    review_id = Column(Integer, primary_key=True, auto_increment=True)
    org_review_id = Column(String(255))
    style_id = Column(String(255), ForeignKey('style.style_id'))
    mall_type_id = Column(Enum(MallType), ForeignKey('style.mall_type_id'))
    crawled_date = Column(Date)
    __table_args__ = (
        ForeignKeyConstraint(
            ['style_id', 'mall_type_id'],
            ['style.style_id', 'style.mall_type_id']
        ),
        UniqueConstraint('org_review_id', 'mall_type_id'),  # 복합 유니크 제약 조건 추가
        {"extend_existing" : True}
    )