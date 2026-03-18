import asyncio
import logging
import os
from secrets import compare_digest

from fastapi import FastAPI, Header, HTTPException, Query, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

from linkedin_scraper import LinkedInScrapeError, LinkedInScraper
from mongo_store import LinkedInJobStore, LinkedInPostStore, MongoStoreError

DEFAULT_LINKEDIN_COMPANY_POSTS_URL = "https://www.linkedin.com/company/moddis-resources/posts/"
DEFAULT_LINKEDIN_REFRESH_MAX_POSTS = 10
WEBSITE_URL = "https://moddisresources.com"

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)s [%(name)s] %(message)s", )
logger = logging.getLogger("moddis.backend")


class StoredLinkedInPost(BaseModel):
    company_url: str
    post_id: str
    text: str
    url: str
    published_at: str | None = None
    author_name: str | None = None
    image_urls: list[str] = Field(default_factory=list)
    source: str | None = None
    scraped_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class StoredLinkedInPostsResponse(BaseModel):
    company_url: str
    post_count: int
    posts: list[StoredLinkedInPost]


class StoredLinkedInJob(BaseModel):
    company_url: str
    job_id: str
    title: str
    location: str | None = None
    description: str | None = None
    apply_email: str | None = None
    company_name: str | None = None
    applicant_count: str | None = None
    published_at: str | None = None
    url: str | None = None
    image_urls: list[str] = Field(default_factory=list)
    employment_type: str | None = None
    seniority_level: str | None = None
    job_function: str | None = None
    industries: str | None = None
    source: str | None = None
    source_post_id: str | None = None
    source_post_url: str | None = None
    scraped_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class StoredLinkedInJobsResponse(BaseModel):
    company_url: str
    job_count: int
    jobs: list[StoredLinkedInJob]


app = FastAPI(title="Moddis Backend", description="Minimal FastAPI backend for storing and serving scraped LinkedIn posts and jobs.", version="1.0.0", docs_url=None, redoc_url=None)
scraper = LinkedInScraper(cache_ttl_seconds=int(os.getenv("LINKEDIN_CACHE_TTL_SECONDS", "900")), timeout_seconds=int(os.getenv("LINKEDIN_REQUEST_TIMEOUT_SECONDS", "20")), )
post_store = LinkedInPostStore()
job_store = LinkedInJobStore()


def is_cron_request(authorization: str | None, x_vercel_cron: str | None) -> bool:
    cron_secret = os.getenv("CRON_SECRET")
    if cron_secret and authorization:
        return compare_digest(authorization, f"Bearer {cron_secret}")
    return bool(x_vercel_cron)


async def refresh_linkedin_content(company_url: str, max_posts: int) -> None:
    posts_payload = await asyncio.to_thread(scraper.get_company_posts, company_url, max_posts, True, )
    posts_storage = await asyncio.to_thread(post_store.save_posts, posts_payload["company_url"], posts_payload["posts"], posts_payload["fetched_at"], )
    jobs_payload = await asyncio.to_thread(scraper.get_company_jobs, posts_payload["company_url"], max_posts, True, )
    jobs_storage = await asyncio.to_thread(job_store.save_jobs, jobs_payload["company_url"], jobs_payload["jobs"], jobs_payload["fetched_at"], )
    logger.info("LinkedIn refresh completed: posts=%s posts_inserted=%s posts_updated=%s jobs=%s jobs_inserted=%s jobs_updated=%s", posts_payload["post_count"], posts_storage["inserted"], posts_storage["updated"], jobs_payload["job_count"], jobs_storage["inserted"], jobs_storage["updated"], )


@app.get("/", include_in_schema=False)
async def read_root() -> RedirectResponse:
    return RedirectResponse(url=WEBSITE_URL, status_code=307)


@app.post("/api/internal/linkedin-refresh", status_code=204, response_model=None)
async def run_linkedin_refresh(authorization: str | None = Header(default=None), x_vercel_cron: str | None = Header(default=None, alias="x-vercel-cron"), company_url: str = Query(default=os.getenv("LINKEDIN_COMPANY_POSTS_URL", DEFAULT_LINKEDIN_COMPANY_POSTS_URL)),
        max_posts: int = Query(default=DEFAULT_LINKEDIN_REFRESH_MAX_POSTS, ge=1, le=25), ) -> Response:
    if not is_cron_request(authorization, x_vercel_cron):
        raise HTTPException(status_code=403, detail="Not authorized to run refresh.")
    if not post_store.enabled or not job_store.enabled:
        raise HTTPException(status_code=503, detail="MongoDB is not configured.")

    try:
        await refresh_linkedin_content(company_url=company_url, max_posts=max_posts)
    except LinkedInScrapeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except MongoStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return Response(status_code=204)


@app.get("/api/linkedin/posts", response_model=StoredLinkedInPostsResponse)
async def get_stored_linkedin_posts(company_url: str = Query(default=os.getenv("LINKEDIN_COMPANY_POSTS_URL", DEFAULT_LINKEDIN_COMPANY_POSTS_URL), description="LinkedIn company URL whose stored posts should be returned.", ),
        limit: int = Query(default=10, ge=1, le=50), ) -> StoredLinkedInPostsResponse:
    canonical_company_url = scraper._build_guest_company_url(company_url)
    try:
        posts = post_store.get_posts(company_url=canonical_company_url, limit=limit)
    except MongoStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return StoredLinkedInPostsResponse(company_url=canonical_company_url, post_count=len(posts), posts=[StoredLinkedInPost.model_validate(post) for post in posts], )


@app.get("/api/linkedin/jobs", response_model=StoredLinkedInJobsResponse)
async def get_stored_linkedin_jobs(company_url: str = Query(default=os.getenv("LINKEDIN_COMPANY_POSTS_URL", DEFAULT_LINKEDIN_COMPANY_POSTS_URL), description="LinkedIn company URL whose stored jobs should be returned.", ),
        limit: int = Query(default=10, ge=1, le=50), ) -> StoredLinkedInJobsResponse:
    canonical_company_url = scraper._build_guest_company_url(company_url)
    try:
        jobs = job_store.get_jobs(company_url=canonical_company_url, limit=limit)
    except MongoStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return StoredLinkedInJobsResponse(company_url=canonical_company_url, job_count=len(jobs), jobs=[StoredLinkedInJob.model_validate(job) for job in jobs], )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=5001, reload=True)
