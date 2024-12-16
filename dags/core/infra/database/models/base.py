from sqlalchemy.orm import declarative_base, registry
from sqlalchemy import MetaData

MySQLBase = declarative_base()
PostgreSQLBase = declarative_base()

reg = registry()
metadata = MetaData()
