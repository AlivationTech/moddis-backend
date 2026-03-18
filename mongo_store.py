from __future__ import annotations

import logging
import os
import re
from datetime import UTC, datetime
from typing import Any

try:
    from pymongo import ASCENDING, DESCENDING, MongoClient
    from pymongo.collection import Collection
    from pymongo.errors import PyMongoError

    PYMONGO_AVAILABLE = True
except ModuleNotFoundError:
    ASCENDING = 1
    DESCENDING = -1
    MongoClient = None
    Collection = Any
    PyMongoError = Exception
    PYMONGO_AVAILABLE = False


class MongoStoreError(Exception):
    pass


logger = logging.getLogger("moddis.mongo")


class LinkedInPostStore:
    def __init__(
        self,
        mongo_uri: str | None = None,
        database_name: str | None = None,
        collection_name: str | None = None,
    ) -> None:
        self.mongo_uri = mongo_uri or os.getenv("MONGODB_URL")
        self.database_name = database_name or os.getenv("MONGODB_DB_NAME", "moddis")
        self.collection_name = collection_name or os.getenv("MONGODB_LINKEDIN_COLLECTION", "linkedin_posts")
        self._client: MongoClient[Any] | None = None
        self._collection: Collection[Any] | None = None

    @property
    def enabled(self) -> bool:
        return bool(PYMONGO_AVAILABLE and self.mongo_uri and self.database_name and self.collection_name)

    def save_posts(self, company_url: str, posts: list[dict[str, Any]], scraped_at: str) -> dict[str, int]:
        self._ensure_available()

        collection = self._get_collection()
        now = datetime.now(UTC).isoformat()
        inserted = 0
        updated = 0

        for post in posts:
            self._delete_legacy_duplicates(collection, company_url, post)
            selector = {
                "company_url": company_url,
                "post_id": post["id"],
            }
            document = {
                "company_url": company_url,
                "post_id": post["id"],
                "text": post["text"],
                "url": post["url"],
                "published_at": post.get("published_at"),
                "author_name": post.get("author_name"),
                "image_urls": post.get("image_urls", []),
                "source": post.get("source"),
                "scraped_at": scraped_at,
                "updated_at": now,
            }
            result = collection.update_one(
                selector,
                {
                    "$set": document,
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
            if result.upserted_id is not None:
                inserted += 1
            elif result.matched_count:
                updated += 1

        return {"inserted": inserted, "updated": updated}

    def get_posts(self, company_url: str, limit: int = 10) -> list[dict[str, Any]]:
        self._ensure_available()

        collection = self._get_collection()
        cursor = collection.find(
            {"company_url": company_url},
            {"_id": 0},
        ).sort(
            [
                ("published_at", DESCENDING),
                ("updated_at", DESCENDING),
                ("created_at", DESCENDING),
            ]
        ).limit(limit)
        return list(cursor)

    def ping(self) -> None:
        self._ensure_available()
        try:
            self._get_collection().database.client.admin.command("ping")
        except PyMongoError as exc:
            raise MongoStoreError(f"MongoDB ping failed: {exc}") from exc

    def _get_collection(self) -> Collection[Any]:
        if self._collection is not None:
            return self._collection
        self._ensure_available()

        try:
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            collection = client[self.database_name][self.collection_name]
            collection.create_index(
                [("company_url", ASCENDING), ("post_id", ASCENDING)],
                unique=True,
                name="company_url_post_id_unique",
            )
            collection.create_index(
                [("company_url", ASCENDING), ("published_at", DESCENDING)],
                name="company_url_published_at_idx",
            )
            logger.info(
                "MongoDB connection established for database '%s' and collection '%s'",
                self.database_name,
                self.collection_name,
            )
        except PyMongoError as exc:
            raise MongoStoreError(f"Could not connect to MongoDB: {exc}") from exc

        self._client = client
        self._collection = collection
        return collection

    def _delete_legacy_duplicates(self, collection: Collection[Any], company_url: str, post: dict[str, Any]) -> None:
        post_url = post.get("url")
        post_id = post.get("id")
        if not isinstance(post_url, str) or not post_url.strip():
            post_url = None
        if not isinstance(post_id, str) or not post_id.strip():
            return
        activity_id = self._extract_activity_id(post_id) or self._extract_activity_id(post_url)
        delete_filter: dict[str, Any] = {
            "company_url": company_url,
            "post_id": {"$ne": post_id},
        }

        or_conditions: list[dict[str, Any]] = []
        if post_url and post_url.rstrip("/") != company_url.rstrip("/"):
            or_conditions.append({"url": post_url})
        if activity_id:
            or_conditions.append({"url": {"$regex": activity_id}})

        if not or_conditions:
            return

        delete_filter["$or"] = or_conditions
        collection.delete_many(delete_filter)

    def _extract_activity_id(self, value: str | None) -> str | None:
        if not isinstance(value, str) or not value.strip():
            return None
        match = re.search(r"activity[:\-](\d+)", value)
        if match is None:
            return None
        return match.group(1)

    def _ensure_available(self) -> None:
        if not PYMONGO_AVAILABLE:
            raise MongoStoreError("PyMongo is not installed. Install dependencies from requirements.txt.")
        if not (self.mongo_uri and self.database_name and self.collection_name):
            raise MongoStoreError("MongoDB is not configured. Set MONGODB_URL to your online MongoDB connection string.")
        if self._is_local_mongo_uri(self.mongo_uri):
            raise MongoStoreError("Local MongoDB URIs are not allowed. Configure MONGODB_URL with the online MongoDB connection string.")

    def _is_local_mongo_uri(self, mongo_uri: str) -> bool:
        normalized_uri = mongo_uri.strip().lower()
        return any(
            marker in normalized_uri
            for marker in (
                "localhost",
                "127.0.0.1",
                "::1",
                "mongodb://mongo:27017",
            )
        )


class LinkedInJobStore:
    def __init__(
        self,
        mongo_uri: str | None = None,
        database_name: str | None = None,
        collection_name: str | None = None,
    ) -> None:
        self.mongo_uri = mongo_uri or os.getenv("MONGODB_URL")
        self.database_name = database_name or os.getenv("MONGODB_DB_NAME", "moddis")
        self.collection_name = collection_name or os.getenv("MONGODB_LINKEDIN_JOBS_COLLECTION", "linkedin_jobs")
        self._client: MongoClient[Any] | None = None
        self._collection: Collection[Any] | None = None

    @property
    def enabled(self) -> bool:
        return bool(PYMONGO_AVAILABLE and self.mongo_uri and self.database_name and self.collection_name)

    def save_jobs(self, company_url: str, jobs: list[dict[str, Any]], scraped_at: str) -> dict[str, int]:
        self._ensure_available()
        collection = self._get_collection()
        now = datetime.now(UTC).isoformat()
        inserted = 0
        updated = 0

        for job in jobs:
            selector = {
                "company_url": company_url,
                "job_id": job["id"],
            }
            document = {
                "company_url": company_url,
                "job_id": job["id"],
                "title": job["title"],
                "location": job.get("location"),
                "description": job.get("description"),
                "apply_email": job.get("apply_email"),
                "company_name": job.get("company_name"),
                "applicant_count": job.get("applicant_count"),
                "published_at": job.get("published_at"),
                "url": job.get("url"),
                "image_urls": job.get("image_urls", []),
                "employment_type": job.get("employment_type"),
                "seniority_level": job.get("seniority_level"),
                "job_function": job.get("job_function"),
                "industries": job.get("industries"),
                "source": job.get("source"),
                "source_post_id": job.get("source_post_id"),
                "source_post_url": job.get("source_post_url"),
                "scraped_at": scraped_at,
                "updated_at": now,
            }
            result = collection.update_one(
                selector,
                {
                    "$set": document,
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
            if result.upserted_id is not None:
                inserted += 1
            elif result.matched_count:
                updated += 1

        return {"inserted": inserted, "updated": updated}

    def get_jobs(self, company_url: str, limit: int = 10) -> list[dict[str, Any]]:
        self._ensure_available()
        collection = self._get_collection()
        cursor = collection.find(
            {"company_url": company_url},
            {"_id": 0},
        ).sort(
            [
                ("published_at", DESCENDING),
                ("updated_at", DESCENDING),
                ("created_at", DESCENDING),
            ]
        ).limit(limit)
        return list(cursor)

    def ping(self) -> None:
        self._ensure_available()
        try:
            self._get_collection().database.client.admin.command("ping")
        except PyMongoError as exc:
            raise MongoStoreError(f"MongoDB ping failed: {exc}") from exc

    def _get_collection(self) -> Collection[Any]:
        if self._collection is not None:
            return self._collection
        self._ensure_available()

        try:
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            collection = client[self.database_name][self.collection_name]
            collection.create_index(
                [("company_url", ASCENDING), ("job_id", ASCENDING)],
                unique=True,
                name="company_url_job_id_unique",
            )
            collection.create_index(
                [("company_url", ASCENDING), ("published_at", DESCENDING)],
                name="company_url_jobs_published_at_idx",
            )
            logger.info(
                "MongoDB connection established for database '%s' and collection '%s'",
                self.database_name,
                self.collection_name,
            )
        except PyMongoError as exc:
            raise MongoStoreError(f"Could not connect to MongoDB: {exc}") from exc

        self._client = client
        self._collection = collection
        return collection

    def _ensure_available(self) -> None:
        if not PYMONGO_AVAILABLE:
            raise MongoStoreError("PyMongo is not installed. Install dependencies from requirements.txt.")
        if not (self.mongo_uri and self.database_name and self.collection_name):
            raise MongoStoreError("MongoDB is not configured. Set MONGODB_URL to your online MongoDB connection string.")
        if self._is_local_mongo_uri(self.mongo_uri):
            raise MongoStoreError("Local MongoDB URIs are not allowed. Configure MONGODB_URL with the online MongoDB connection string.")

    def _is_local_mongo_uri(self, mongo_uri: str) -> bool:
        normalized_uri = mongo_uri.strip().lower()
        return any(
            marker in normalized_uri
            for marker in (
                "localhost",
                "127.0.0.1",
                "::1",
                "mongodb://mongo:27017",
            )
        )
