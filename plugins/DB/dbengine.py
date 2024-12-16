from urllib.parse import quote
from sqlalchemy import create_engine

user = "root"
pwd = "1234"
host = "localhost"
port = 3307
db_url = f'mysql+pymysql://{user}:{quote(pwd)}@{host}:{port}'

engine = create_engine(db_url, pool_size=5, max_overflow=5, echo=True)