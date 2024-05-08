from functools import wraps
from airflow.providers.mongo.hooks.mongo import MongoHook
import pendulum
import logging

logger = logging.getLogger(__name__)


class MongoResponseCache:
    def __init__(
        self,
        mongodb_conn_id: str = "aws.documentdb",
        db: str = "cache",
        collection: str = "response",
        key: str | None = None,
        type: str = "json",
    ):
        self.hook = MongoHook(mongo_conn_id=mongodb_conn_id)
        self.client = self.hook.get_conn()
        self.db = self.client[db]
        self.collection = self.db[collection]
        self.key = key
        self.type = type

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            assert kwargs.get("url", None) is not None
            search_key = kwargs
            if self.key:
                search_key["key"] = self.key
            logger.debug(search_key)

            cached_result = self.collection.find_one(search_key)

            if cached_result:
                print(f"hit : {search_key}")
                return cached_result[self.type]
            value = func(*args, **kwargs)
            search_key[self.type] = value
            search_key["created_at"] = pendulum.now("UTC")
            self.collection.insert_one(search_key)
            return value

        return wrapper


class MongoResponseAsyncCache:
    def __init__(
        self,
        mongodb_conn_id: str = "aws.documentdb",
        db: str = "cache",
        collection: str = "response",
        key: str | None = None,
        type: str = "json",
    ):
        self.hook = MongoHook(mongo_conn_id=mongodb_conn_id)
        self.client = self.hook.get_conn()
        self.db = self.client[db]
        self.collection = self.db[collection]
        self.key = key
        self.type = type

    async def __call__(self, func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            assert kwargs.get("url", None) is not None
            search_key = kwargs
            if self.key:
                search_key["key"] = self.key
            logger.debug(search_key)

            cached_result = self.collection.find_one(search_key)

            if cached_result:
                print(f"hit : {search_key}")
                return cached_result[self.type]
            value = await func(*args, **kwargs)
            search_key[self.type] = value
            search_key["created_at"] = pendulum.now("UTC")
            self.collection.insert_one(search_key)
            return value

        return wrapper