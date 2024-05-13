from sqlalchemy.orm import declarative_base, registry
from sqlalchemy import MetaData

Base = declarative_base()

reg = registry()
metadata = MetaData()
