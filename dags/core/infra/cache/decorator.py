from functools import wraps
from airflow.providers.mongo.hooks.mongo import MongoHook
import pendulum, logging, json, hashlib, traceback
from datetime import datetime
import asyncio
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Seoul")

class MongoResponseCache:
    def __init__(
        self,
        mongodb_conn_id: str = "aws.documentdb",
        db: str = "cache",
        collection: str = "response",
        key: str | None = None,
        type: str = "json",
        payload = False,
        date_key = None
    ):
        self.hook = MongoHook(mongo_conn_id=mongodb_conn_id)
        self.client = self.hook.get_conn()
        self.db = self.client[db]
        self.collection = self.db[collection]
        self.key = key
        self.type = type
        self.payload = payload
        self.date_key = date_key

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            assert kwargs.get("url", None) is not None
            search_key = {'url': kwargs['url']}  
            if self.key:
                search_key["key"] = self.key  

            if self.date_key:
                search_key['date_key'] = kwargs.get(self.date_key)

            if self.payload:
                payload = kwargs.get("payload", {})
                search_key['payload'] = payload

            cached_result = self.collection.find_one(search_key)

            if cached_result:
                return cached_result[self.type]

            try:
                value = func(*args, **kwargs)
                if "upstream connect error" in value:
                    logger.error("Connection error occurred. Not caching the result.")
                    raise ValueError("Connection error, not caching this result.")
            except Exception as e:
                logger.error(f"Error during fetching data: {e}")
                logger.error(traceback.format_exc())
                raise AirflowException("An error occurred, marking task for retry.")


            search_key[self.type] = value
            
            self.collection.insert_one(search_key)
            return value

        return wrapper
    
    
    
class MongoAsyncResponseCache:
    def __init__(
        self,
        mongodb_conn_id: str = "aws.documentdb",
        db: str = "cache",
        collection: str = "response",
        key: str | None = None,
        type: str = "json",
        payload = False,
        date_key = None
    ):
        self.hook = MongoHook(mongo_conn_id=mongodb_conn_id)
        self.client = self.hook.get_conn()
        self.db = self.client[db]
        self.collection = self.db[collection]
        self.key = key
        self.type = type
        self.payload = payload
        self.date_key = date_key

    def __call__(self, func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            assert kwargs.get("url", None) is not None
            search_key = {'url': kwargs['url']}  
            if self.key:
                search_key["key"] = self.key  

            if self.date_key:
                search_key['date_key'] = kwargs.get(self.date_key)

            if self.payload:
                payload = kwargs.get("payload", {})
                search_key['payload'] = payload

            cached_result = self.collection.find_one(search_key)

            if cached_result:
                return cached_result[self.type]

            try:
                value = await func(*args, **kwargs)
                if "upstream connect error" in value:
                    logger.error("Connection error occurred. Not caching the result.")
                    raise ValueError("Connection error, not caching this result.")
            except Exception as e:
                logger.error(f"Error during fetching data: {e}")
                logger.error(traceback.format_exc())
                raise AirflowException("An error occurred, marking task for retry.")


            search_key[self.type] = value
            
            self.collection.insert_one(search_key)
            return value

        return wrapper
