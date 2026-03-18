from __future__ import annotations

import hashlib
import html
import json
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

LINKEDIN_BASE_URL = "https://www.linkedin.com"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/132.0.0.0 Safari/537.36"
)
MAX_JSON_FRAGMENTS_PER_BLOCK = 30
POST_MARKER_KEYS = {
    "activity",
    "articleBody",
    "commentary",
    "createdAt",
    "datePublished",
    "entityUrn",
    "permalink",
    "publishedAt",
    "shareCommentary",
    "text",
    "url",
}

SCRIPT_PATTERN = re.compile(r"<script\b[^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)
JSON_LD_PATTERN = re.compile(
    r'<script\b[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
ARTICLE_CARD_PATTERN = re.compile(
    r'(?is)<article[^>]*data-activity-urn="(?P<activity>[^"]+)"[^>]*>(?P<body>.*?)</article>'
)
UPDATES_SECTION_PATTERN = re.compile(
    r'(?is)<section[^>]*class="[^"]*\bupdates\b[^"]*"[^>]*>(?P<body>.*?)</section>'
)
JOBS_SECTION_PATTERN = re.compile(
    r'(?is)<section[^>]*data-test-id="jobs-at"[^>]*>(?P<body>.*?)</section>'
)
MAIN_JOB_CARD_PATTERN = re.compile(
    r'(?is)<div[^>]*class="[^"]*\bmain-job-card\b[^"]*"[^>]*data-entity-urn="(?P<urn>[^"]+)"[^>]*>(?P<body>.*?)</div>\s*</li>'
)
CODE_PATTERN = re.compile(r"<code\b[^>]*>(.*?)</code>", re.IGNORECASE | re.DOTALL)
TAG_PATTERN = re.compile(r"<[^>]+>")
WHITESPACE_PATTERN = re.compile(r"\s+")


class LinkedInScrapeError(Exception):
    pass


@dataclass(slots=True)
class CacheEntry:
    expires_at: float
    payload: dict[str, Any]


class LinkedInScraper:
    def __init__(self, cache_ttl_seconds: int = 900, timeout_seconds: int = 20) -> None:
        self.cache_ttl_seconds = cache_ttl_seconds
        self.timeout_seconds = timeout_seconds
        self.user_agent = os.getenv("LINKEDIN_SCRAPER_USER_AGENT", DEFAULT_USER_AGENT)
        self._cache: dict[str, CacheEntry] = {}
        self._cache_lock = threading.Lock()

    def get_company_posts(
        self,
        company_url: str,
        max_posts: int = 10,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        canonical_company_url = self._build_guest_company_url(company_url)
        posts_page_url = self._build_posts_page_url(company_url)
        cache_key = f"{posts_page_url}|{max_posts}"

        if not force_refresh:
            cached = self._get_cached(cache_key)
            if cached is not None:
                cached["cached"] = True
                return cached

        html_text = self._download_html(posts_page_url)
        fetched_at = datetime.now(UTC)
        posts = self._extract_posts(html_text, canonical_company_url, max_posts=max_posts, fetched_at=fetched_at)

        if not posts and self._looks_like_login_page(html_text):
            guest_company_url = canonical_company_url
            guest_html_text = self._download_html(guest_company_url)
            posts = self._extract_posts(
                guest_html_text,
                guest_company_url,
                max_posts=max_posts,
                fetched_at=fetched_at,
            )

        payload = {
            "company_url": canonical_company_url,
            "cached": False,
            "fetched_at": fetched_at.isoformat(),
            "post_count": len(posts),
            "posts": posts,
            "warning": None
            if posts
            else (
                "No posts were parsed from the public page. LinkedIn markup changes often "
                "and public access may be rate-limited or blocked."
            ),
        }
        self._set_cached(cache_key, payload)
        return payload

    def get_company_jobs(
        self,
        company_url: str,
        max_jobs: int = 10,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        canonical_company_url = self._build_guest_company_url(company_url)
        jobs_page_url = self._build_jobs_page_url(company_url)
        cache_key = f"{jobs_page_url}|jobs|{max_jobs}"

        if not force_refresh:
            cached = self._get_cached(cache_key)
            if cached is not None:
                cached["cached"] = True
                return cached

        fetched_at = datetime.now(UTC)
        html_text = self._download_html(jobs_page_url)
        jobs = self._extract_company_page_jobs(
            html_text=html_text,
            company_url=canonical_company_url,
            max_jobs=max_jobs,
            fetched_at=fetched_at,
        )

        warning = None
        source_company_url = canonical_company_url

        if not jobs:
            posts_payload = self.get_company_posts(
                company_url=canonical_company_url,
                max_posts=max(max_jobs * 3, max_jobs),
                force_refresh=force_refresh,
            )
            jobs = self._extract_jobs_from_posts(posts_payload["posts"], max_jobs=max_jobs)
            source_company_url = posts_payload["company_url"]
            warning = (
                None
                if jobs
                else (
                    "No structured jobs were derived from public LinkedIn content. "
                    "The company jobs page does not currently expose public job cards in this runtime."
                )
            )

        payload = {
            "company_url": source_company_url,
            "cached": False,
            "fetched_at": fetched_at.isoformat(),
            "job_count": len(jobs),
            "warning": warning,
            "jobs": jobs,
        }
        self._set_cached(cache_key, payload)
        return {
            "company_url": payload["company_url"],
            "cached": payload["cached"],
            "fetched_at": payload["fetched_at"],
            "job_count": payload["job_count"],
            "warning": payload["warning"],
            "jobs": payload["jobs"],
        }

    def _get_cached(self, cache_key: str) -> dict[str, Any] | None:
        with self._cache_lock:
            entry = self._cache.get(cache_key)
            if entry is None:
                return None
            if entry.expires_at <= time.time():
                self._cache.pop(cache_key, None)
                return None
            return dict(entry.payload)

    def _set_cached(self, cache_key: str, payload: dict[str, Any]) -> None:
        with self._cache_lock:
            self._cache[cache_key] = CacheEntry(
                expires_at=time.time() + self.cache_ttl_seconds,
                payload=dict(payload),
            )

    def _download_html(self, url: str) -> str:
        request = Request(
            url,
            headers={
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                content_type = response.headers.get_content_charset() or "utf-8"
                return response.read().decode(content_type, errors="replace")
        except HTTPError as exc:
            raise LinkedInScrapeError(
                f"LinkedIn returned HTTP {exc.code}. Public scraping may be blocked for this page."
            ) from exc
        except URLError as exc:
            raise LinkedInScrapeError(f"Could not reach LinkedIn: {exc.reason}") from exc

    def _extract_posts(
        self,
        html_text: str,
        company_url: str,
        max_posts: int,
        fetched_at: datetime,
    ) -> list[dict[str, Any]]:
        posts_by_id: dict[str, dict[str, Any]] = {}

        guest_posts = self._extract_guest_page_posts(html_text, company_url, max_posts=max_posts, fetched_at=fetched_at)
        for post in guest_posts:
            self._merge_post(posts_by_id, post)
            if len(posts_by_id) >= max_posts:
                return self._sorted_posts(posts_by_id, max_posts)

        for raw_json in JSON_LD_PATTERN.findall(html_text):
            try:
                parsed = json.loads(html.unescape(raw_json.strip()))
            except json.JSONDecodeError:
                continue
            for post in self._post_candidates_from_json(parsed, company_url, source="json_ld"):
                self._merge_post(posts_by_id, post)
                if len(posts_by_id) >= max_posts:
                    return self._sorted_posts(posts_by_id, max_posts)

        html_without_json_ld = JSON_LD_PATTERN.sub("", html_text)
        blocks = SCRIPT_PATTERN.findall(html_without_json_ld) + CODE_PATTERN.findall(html_text)
        for block in blocks:
            for fragment in self._extract_json_fragments(html.unescape(block)):
                for post in self._post_candidates_from_json(fragment, company_url, source="embedded_json"):
                    self._merge_post(posts_by_id, post)
                    if len(posts_by_id) >= max_posts:
                        return self._sorted_posts(posts_by_id, max_posts)

        return self._sorted_posts(posts_by_id, max_posts)

    def _extract_jobs_from_posts(self, posts: list[dict[str, Any]], max_jobs: int) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []

        for post in posts:
            text = post.get("text", "")
            if not self._looks_like_job_post(text):
                continue

            raw_title = self._extract_job_title(text)
            title, title_location = self._split_job_title_location(raw_title)
            if not title:
                continue

            jobs.append(
                {
                    "id": post["id"],
                    "title": title,
                    "location": title_location or self._extract_job_location(text),
                    "description": text,
                    "apply_email": self._extract_apply_email(text),
                    "company_name": post.get("author_name"),
                    "applicant_count": None,
                    "published_at": post.get("published_at"),
                    "url": post.get("url"),
                    "image_urls": post.get("image_urls", []),
                    "employment_type": None,
                    "seniority_level": None,
                    "job_function": None,
                    "industries": None,
                    "source": "linkedin_post",
                    "source_post_id": post["id"],
                    "source_post_url": post.get("url"),
                }
            )

            if len(jobs) >= max_jobs:
                break

        return jobs

    def _extract_company_page_jobs(
        self,
        html_text: str,
        company_url: str,
        max_jobs: int,
        fetched_at: datetime,
    ) -> list[dict[str, Any]]:
        section_match = JOBS_SECTION_PATTERN.search(html_text)
        if section_match is None:
            return []

        section_html = section_match.group("body")
        jobs: list[dict[str, Any]] = []

        for match in MAIN_JOB_CARD_PATTERN.finditer(section_html):
            job_urn = html.unescape(match.group("urn")).strip()
            card_html = match.group(0)
            title = self._extract_first_match(
                card_html,
                r'(?is)<h3[^>]*class="[^"]*\bbase-main-card__title\b[^"]*"[^>]*>(?P<value>.*?)</h3>',
            )
            if not title:
                continue

            job_url = self._extract_first_match(
                card_html,
                r'(?is)<a[^>]*class="[^"]*\bbase-card__full-link\b[^"]*"[^>]*href="(?P<value>[^"]+)"',
            )
            normalized_job_url = self._normalize_post_url(job_url) if job_url else self._activity_urn_to_url(job_urn)

            company_name = self._extract_first_match(
                card_html,
                r'(?is)<h4[^>]*class="[^"]*\bbase-main-card__subtitle\b[^"]*"[^>]*>.*?<a[^>]*>(?P<value>.*?)</a>',
            )
            location = self._extract_first_match(
                card_html,
                r'(?is)<span[^>]*class="[^"]*\bmain-job-card__location\b[^"]*"[^>]*>(?P<value>.*?)</span>',
            )
            published_at = self._extract_first_match(
                card_html,
                r'(?is)<time[^>]*datetime="(?P<value>[^"]+)"',
            )
            if published_at:
                published_at = self._normalize_timestamp(published_at)
            else:
                relative_time = self._extract_first_match(card_html, r'(?is)<time\b[^>]*>(?P<value>.*?)</time>')
                published_at = self._normalize_human_relative_timestamp(relative_time, fetched_at) if relative_time else None

            logo_url = self._extract_first_match(
                card_html,
                r'(?is)<img\b[^>]*(?:data-delayed-url|src)="(?P<value>[^"]+)"',
            )
            image_urls = []
            if logo_url:
                normalized_logo_url = self._normalize_post_url(logo_url)
                if normalized_logo_url:
                    image_urls.append(normalized_logo_url)

            job = {
                "id": job_urn,
                "title": title,
                "location": location,
                "description": title,
                "apply_email": None,
                "company_name": company_name,
                "applicant_count": None,
                "published_at": published_at,
                "url": normalized_job_url,
                "image_urls": image_urls,
                "employment_type": None,
                "seniority_level": None,
                "job_function": None,
                "industries": None,
                "source": "company_jobs_page",
                "source_post_id": job_urn,
                "source_post_url": normalized_job_url,
            }
            jobs.append(self._enrich_job_with_detail_page(job))
            if len(jobs) >= max_jobs:
                break

        return jobs

    def _enrich_job_with_detail_page(self, job: dict[str, Any]) -> dict[str, Any]:
        job_url = job.get("url")
        if not isinstance(job_url, str) or not job_url.strip():
            return job

        try:
            detail_html = self._download_html(job_url)
        except LinkedInScrapeError:
            return job

        description_html = self._extract_first_match(
            detail_html,
            r'(?is)<div[^>]*class="[^"]*\bshow-more-less-html__markup\b[^"]*"[^>]*>(?P<value>.*?)</div>',
        )
        if description_html:
            job["description"] = self._clean_text(description_html)
            job["apply_email"] = self._extract_apply_email(job["description"])

        applicants = self._extract_first_match(
            detail_html,
            r'(?is)<figcaption[^>]*class="[^"]*\bnum-applicants__caption\b[^"]*"[^>]*>(?P<value>.*?)</figcaption>',
        )
        if applicants:
            job["applicant_count"] = applicants

        for label, value in re.findall(
            r'(?is)<li[^>]*class="[^"]*\bdescription__job-criteria-item\b[^"]*"[^>]*>.*?'
            r'<h3[^>]*class="[^"]*\bdescription__job-criteria-subheader\b[^"]*"[^>]*>(?P<label>.*?)</h3>.*?'
            r'<span[^>]*class="[^"]*\bdescription__job-criteria-text--criteria\b[^"]*"[^>]*>(?P<value>.*?)</span>.*?</li>',
            detail_html,
        ):
            normalized_label = self._clean_text(label).lower()
            normalized_value = self._clean_text(value)
            if normalized_label == "seniority level":
                job["seniority_level"] = normalized_value
            elif normalized_label == "employment type":
                job["employment_type"] = normalized_value
            elif normalized_label == "job function":
                job["job_function"] = normalized_value
            elif normalized_label == "industries":
                job["industries"] = normalized_value

        top_company_name = self._extract_first_match(
            detail_html,
            r'(?is)<title>(?P<value>.*?) hiring ',
        )
        if top_company_name and not job.get("company_name"):
            job["company_name"] = top_company_name

        return job

    def _extract_guest_page_posts(
        self,
        html_text: str,
        company_url: str,
        max_posts: int,
        fetched_at: datetime,
    ) -> list[dict[str, Any]]:
        section_match = UPDATES_SECTION_PATTERN.search(html_text)
        if section_match is None:
            return []

        section_html = section_match.group("body")
        posts: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        for match in ARTICLE_CARD_PATTERN.finditer(section_html):
            activity_urn = html.unescape(match.group("activity"))
            card_html = match.group("body")
            post_id = activity_urn.strip()
            if post_id in seen_ids:
                continue

            text_match = re.search(
                r'(?is)data-test-id="main-feed-activity-card__commentary"[^>]*>(?P<text>.*?)</p>',
                card_html,
            )
            if text_match is None:
                continue

            text = self._clean_text(text_match.group("text"))
            if len(text) < 20:
                continue

            post_url = self._extract_post_url_from_card(section_html, match.start(), match.end(), activity_urn, company_url)

            author_match = re.search(
                r'(?is)aria-label="View organization page for (?P<author>[^"]+)"',
                card_html,
            )
            author_name = self._clean_text(author_match.group("author")) if author_match is not None else None

            time_match = re.search(r'(?is)<time\b[^>]*>(?P<time>.*?)</time>', card_html)
            published_at = None
            if time_match is not None:
                published_at = self._normalize_relative_timestamp(
                    self._clean_text(time_match.group("time")),
                    fetched_at,
                )

            image_urls = self._extract_guest_card_images(card_html)

            posts.append(
                {
                    "id": post_id,
                    "text": text,
                    "url": post_url,
                    "published_at": published_at,
                    "author_name": author_name,
                    "image_urls": self._dedupe_preserve_order(image_urls),
                    "source": "guest_html",
                }
            )
            seen_ids.add(post_id)

            if len(posts) >= max_posts:
                break

        return posts

    def _extract_post_url_from_card(
        self,
        section_html: str,
        match_start: int,
        match_end: int,
        activity_urn: str,
        company_url: str,
    ) -> str:
        search_window_start = max(0, match_start - 1200)
        search_window_end = min(len(section_html), match_end + 400)
        surrounding_html = section_html[search_window_start:search_window_end]
        link_match = re.search(
            r'(?is)data-id="main-feed-card__full-link"[^>]*href="(?P<href>[^"]+)"',
            surrounding_html,
        )
        if link_match is not None:
            return urljoin(LINKEDIN_BASE_URL, html.unescape(link_match.group("href")))

        activity_url = self._activity_urn_to_url(activity_urn)
        return activity_url or company_url

    def _extract_guest_card_images(self, card_html: str) -> list[str]:
        image_urls: list[str] = []
        media_match = re.search(
            r'(?is)<ul\b[^>]*data-test-id="feed-images-content"[^>]*>(?P<media>.*?)</ul>',
            card_html,
        )
        if media_match is None:
            return image_urls

        media_html = media_match.group("media")
        for image_match in re.finditer(
            r'(?is)<img\b[^>]*(?:data-delayed-url|src)="(?P<url>[^"]+)"',
            media_html,
        ):
            image_url = self._normalize_post_url(html.unescape(image_match.group("url")))
            if image_url and "media.licdn.com" in image_url and "company-logo_" not in image_url:
                image_urls.append(image_url)

        return self._dedupe_preserve_order(image_urls)

    def _extract_json_fragments(self, text: str) -> list[Any]:
        decoder = json.JSONDecoder()
        fragments: list[Any] = []
        index = 0
        text_length = len(text)

        while index < text_length and len(fragments) < MAX_JSON_FRAGMENTS_PER_BLOCK:
            char = text[index]
            if char not in "[{":
                index += 1
                continue

            try:
                parsed, offset = decoder.raw_decode(text[index:])
            except json.JSONDecodeError:
                index += 1
                continue

            fragments.append(parsed)
            index += offset

        return fragments

    def _post_candidates_from_json(
        self,
        payload: Any,
        company_url: str,
        source: str,
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        seen_candidate_ids: set[str] = set()

        for node in self._walk_json(payload):
            if not isinstance(node, dict):
                continue
            if not any(key in node for key in POST_MARKER_KEYS):
                continue

            text = self._extract_text(node)
            if len(text) < 20:
                continue

            post_url = self._extract_url(node, company_url)
            published_at = self._extract_timestamp(node)
            author = self._extract_author(node)
            image_urls = self._extract_images(node, company_url)

            if not post_url and not published_at:
                continue

            if "/posts/" not in post_url and "ugcPost" not in json.dumps(node, default=str):
                continue

            candidate_id = self._build_candidate_id(node, post_url, text, published_at)
            if candidate_id in seen_candidate_ids:
                continue
            seen_candidate_ids.add(candidate_id)
            candidates.append(
                {
                    "id": candidate_id,
                    "text": text,
                    "url": post_url or company_url,
                    "published_at": published_at,
                    "author_name": author,
                    "image_urls": image_urls,
                    "source": source,
                }
            )

        candidates.sort(
            key=lambda post: post["published_at"] or "",
            reverse=True,
        )
        return candidates

    def _walk_json(self, payload: Any) -> list[Any]:
        nodes: list[Any] = []
        stack = [payload]
        seen: set[int] = set()

        while stack:
            current = stack.pop()
            current_id = id(current)
            if current_id in seen:
                continue
            seen.add(current_id)
            nodes.append(current)

            if isinstance(current, dict):
                stack.extend(current.values())
            elif isinstance(current, list):
                stack.extend(current)

        return nodes

    def _extract_text(self, node: dict[str, Any]) -> str:
        for key in (
            "commentary",
            "shareCommentary",
            "articleBody",
            "text",
            "title",
            "description",
            "content",
            "summary",
        ):
            text = self._extract_text_value(node.get(key))
            if len(text) >= 20:
                return text

        for value in node.values():
            text = self._extract_text_value(value)
            if (
                len(text) >= 20
                and "linkedin" not in text.lower()
                and not self._looks_like_timestamp(text)
            ):
                return text

        return ""

    def _extract_text_value(self, value: Any) -> str:
        if isinstance(value, str):
            return self._clean_text(value)

        if isinstance(value, dict):
            for key in ("text", "accessibleText", "title", "name", "value", "commentary"):
                nested = self._extract_text_value(value.get(key))
                if nested:
                    return nested

            for nested_value in value.values():
                nested = self._extract_text_value(nested_value)
                if nested:
                    return nested

        if isinstance(value, list):
            for item in value:
                nested = self._extract_text_value(item)
                if nested:
                    return nested

        return ""

    def _extract_url(self, node: dict[str, Any], company_url: str) -> str | None:
        for key in ("permalink", "url", "navigationUrl", "canonicalUrl", "entityUrn"):
            value = node.get(key)
            resolved = self._normalize_post_url(value)
            if resolved:
                return resolved

        for value in node.values():
            if isinstance(value, dict):
                resolved = self._extract_url(value, company_url)
                if resolved:
                    return resolved

        return None

    def _normalize_post_url(self, value: Any) -> str | None:
        if not isinstance(value, str) or not value.strip():
            return None

        raw_value = value.strip()
        if raw_value.startswith("urn:li:activity:"):
            activity_id = raw_value.rsplit(":", maxsplit=1)[-1]
            return f"{LINKEDIN_BASE_URL}/feed/update/urn:li:activity:{activity_id}"

        if raw_value.startswith("/"):
            return urljoin(LINKEDIN_BASE_URL, raw_value)

        if raw_value.startswith("http://") or raw_value.startswith("https://"):
            return raw_value

        return None

    def _extract_timestamp(self, node: dict[str, Any]) -> str | None:
        for key in (
            "publishedAt",
            "publishedOn",
            "createdAt",
            "createdOn",
            "lastModifiedAt",
            "datePublished",
        ):
            raw_value = node.get(key)
            parsed = self._normalize_timestamp(raw_value)
            if parsed:
                return parsed

        for value in node.values():
            if isinstance(value, dict):
                parsed = self._extract_timestamp(value)
                if parsed:
                    return parsed

        return None

    def _normalize_timestamp(self, value: Any) -> str | None:
        if value is None:
            return None

        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            try:
                parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                return parsed.isoformat()
            except ValueError:
                if stripped.isdigit():
                    value = int(stripped)
                else:
                    return None

        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 1_000_000_000_000:
                timestamp /= 1000
            try:
                return datetime.fromtimestamp(timestamp, tz=UTC).isoformat()
            except (OverflowError, OSError, ValueError):
                return None

        return None

    def _normalize_relative_timestamp(self, value: str, fetched_at: datetime) -> str | None:
        cleaned = value.strip().lower()
        if not cleaned:
            return None

        match = re.fullmatch(r"(?P<count>\d+)\s*(?P<unit>m|h|d|w|mo|yr)", cleaned)
        if match is None:
            return None

        count = int(match.group("count"))
        unit = match.group("unit")
        seconds_by_unit = {
            "m": 60,
            "h": 60 * 60,
            "d": 24 * 60 * 60,
            "w": 7 * 24 * 60 * 60,
            "mo": 30 * 24 * 60 * 60,
            "yr": 365 * 24 * 60 * 60,
        }
        delta_seconds = count * seconds_by_unit[unit]
        return datetime.fromtimestamp(fetched_at.timestamp() - delta_seconds, tz=UTC).isoformat()

    def _normalize_human_relative_timestamp(self, value: str, fetched_at: datetime) -> str | None:
        if not value:
            return None
        cleaned = value.strip().lower()
        match = re.fullmatch(r"(?P<count>\d+)\s+(?P<unit>hour|hours|day|days|week|weeks|month|months|year|years)\s+ago", cleaned)
        if match is None:
            return None
        count = int(match.group("count"))
        unit = match.group("unit")
        unit_seconds = {
            "hour": 60 * 60,
            "hours": 60 * 60,
            "day": 24 * 60 * 60,
            "days": 24 * 60 * 60,
            "week": 7 * 24 * 60 * 60,
            "weeks": 7 * 24 * 60 * 60,
            "month": 30 * 24 * 60 * 60,
            "months": 30 * 24 * 60 * 60,
            "year": 365 * 24 * 60 * 60,
            "years": 365 * 24 * 60 * 60,
        }
        return datetime.fromtimestamp(fetched_at.timestamp() - count * unit_seconds[unit], tz=UTC).isoformat()

    def _extract_author(self, node: dict[str, Any]) -> str | None:
        for key in ("actorName", "author", "name"):
            value = node.get(key)
            if isinstance(value, str):
                cleaned = self._clean_text(value)
                if cleaned and len(cleaned) < 120:
                    return cleaned
            if isinstance(value, dict):
                nested = self._extract_text_value(value)
                if nested and len(nested) < 120:
                    return nested

        return None

    def _extract_images(self, node: dict[str, Any], company_url: str) -> list[str]:
        image_urls: list[str] = []
        for key in ("image", "images", "thumbnail", "vectorImage"):
            image_urls.extend(self._extract_image_values(node.get(key), company_url))

        deduped: list[str] = []
        seen: set[str] = set()
        for image_url in image_urls:
            if image_url not in seen:
                seen.add(image_url)
                deduped.append(image_url)

        return deduped

    def _extract_image_values(self, value: Any, company_url: str) -> list[str]:
        urls: list[str] = []

        if isinstance(value, str):
            normalized = self._normalize_post_url(value)
            if normalized:
                urls.append(normalized)
            return urls

        if isinstance(value, dict):
            for key in ("url", "rootUrl"):
                normalized = self._normalize_post_url(value.get(key))
                if normalized:
                    urls.append(normalized)

            artifacts = value.get("artifacts")
            if isinstance(artifacts, list):
                root_url = value.get("rootUrl")
                if isinstance(root_url, str):
                    for artifact in artifacts:
                        segment = artifact.get("fileIdentifyingUrlPathSegment") if isinstance(artifact, dict) else None
                        if isinstance(segment, str):
                            urls.append(f"{root_url}{segment}")

            for nested in value.values():
                urls.extend(self._extract_image_values(nested, company_url))
            return urls

        if isinstance(value, list):
            for item in value:
                urls.extend(self._extract_image_values(item, company_url))

        return urls

    def _build_candidate_id(
        self,
        node: dict[str, Any],
        post_url: str | None,
        text: str,
        published_at: str | None,
    ) -> str:
        for key in ("entityUrn", "trackingId", "id", "urn"):
            raw_value = node.get(key)
            if isinstance(raw_value, str) and raw_value.strip():
                canonical = self._canonical_post_id(raw_value.strip(), post_url)
                if canonical:
                    return canonical

        canonical_from_url = self._canonical_post_id(None, post_url)
        if canonical_from_url:
            return canonical_from_url

        fingerprint = "|".join(part for part in (post_url or "", published_at or "", text[:120]) if part)
        return hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()

    def _canonical_post_id(self, raw_id: str | None, post_url: str | None) -> str | None:
        for value in (raw_id, post_url):
            activity_urn = self._extract_activity_urn(value)
            if activity_urn:
                return activity_urn
        return raw_id

    def _extract_activity_urn(self, value: str | None) -> str | None:
        if not isinstance(value, str) or not value.strip():
            return None

        raw_value = html.unescape(value.strip())
        urn_match = re.search(r"urn:li:activity:(\d+)", raw_value)
        if urn_match:
            return f"urn:li:activity:{urn_match.group(1)}"

        url_match = re.search(r"activity-(\d+)", raw_value)
        if url_match:
            return f"urn:li:activity:{url_match.group(1)}"

        parsed = urlparse(raw_value)
        if parsed.path:
            path_match = re.search(r"activity-(\d+)", parsed.path)
            if path_match:
                return f"urn:li:activity:{path_match.group(1)}"

        return None

    def _activity_urn_to_url(self, activity_urn: str | None) -> str | None:
        canonical_urn = self._extract_activity_urn(activity_urn)
        if canonical_urn is None:
            return None
        return f"{LINKEDIN_BASE_URL}/feed/update/{canonical_urn}"

    def _normalize_company_url(self, company_url: str) -> str:
        normalized = company_url.strip()
        if not normalized:
            raise LinkedInScrapeError("A LinkedIn company posts URL is required.")

        if not normalized.startswith("http://") and not normalized.startswith("https://"):
            normalized = f"https://{normalized.lstrip('/')}"

        if "linkedin.com" not in normalized:
            raise LinkedInScrapeError("The URL must point to a LinkedIn company page.")

        parsed = urlparse(normalized)
        match = re.search(r"/company/([^/?#]+)/?", parsed.path)
        if match is None:
            raise LinkedInScrapeError("The URL must point to a LinkedIn company page.")

        return f"{parsed.scheme}://{parsed.netloc}/company/{match.group(1)}/"

    def _build_guest_company_url(self, company_url: str) -> str:
        return self._normalize_company_url(company_url)

    def _build_posts_page_url(self, company_url: str) -> str:
        return self._build_guest_company_url(company_url).rstrip("/") + "/posts/"

    def _build_jobs_page_url(self, company_url: str) -> str:
        return self._build_guest_company_url(company_url).rstrip("/") + "/jobs/"

    def _looks_like_login_page(self, html_text: str) -> bool:
        lowered = html_text.lower()
        return (
            "linkedin login, sign in" in lowered
            or 'pagekey" content="d_checkpoint' in lowered
            or 'href="https://www.linkedin.com/login' in lowered
            or "<title>linkedin login" in lowered
        )

    def _dedupe_preserve_order(self, values: list[str]) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for value in values:
            if value not in seen:
                seen.add(value)
                deduped.append(value)
        return deduped

    def _looks_like_job_post(self, text: str) -> bool:
        lowered = text.lower()
        return (
            "#hiring" in lowered
            or "we are recruiting" in lowered
            or "apply by sending your cv" in lowered
            or "subject of your email" in lowered
        )

    def _extract_job_title(self, text: str) -> str | None:
        normalized = " ".join(text.split())
        patterns = [
            r"(?i)(?:we['’]?re|we are)\s*#hiring\s*!*\s*(?P<title>.+?)(?:\s+(?:we are|are you|we’re looking|we're looking|a financial services provider|this role|if you are|if you enjoy|you['’]ll))",
            r"(?i)#hiring\s*!*\s*(?P<title>.+?)(?:\s+(?:we are|are you|we’re looking|we're looking|a financial services provider|this role|if you are|if you enjoy|you['’]ll))",
        ]
        for pattern in patterns:
            match = re.search(pattern, normalized)
            if match is not None:
                title = match.group("title").strip(" -:!.,")
                title = re.sub(r"\s{2,}", " ", title)
                if 3 <= len(title) <= 120:
                    return title
        return None

    def _split_job_title_location(self, title: str | None) -> tuple[str | None, str | None]:
        if not title:
            return None, None
        match = re.search(r"(?i)^(?P<title>.+?)\s*[–-]\s*(?P<location>Lagos|Abuja|Nigeria|Remote)$", title.strip())
        if match is None:
            return title, None
        return match.group("title").strip(), match.group("location").strip()

    def _extract_job_location(self, text: str) -> str | None:
        patterns = [
            r"(?i)\b(?P<location>[A-Za-z ]+)-based\b",
            r"(?i)\bin\s+(?P<location>Lagos|Abuja|Nigeria|Remote)\b",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match is not None:
                location = " ".join(match.group("location").split()).strip(" -,.")
                if location:
                    return location
        return None

    def _extract_apply_email(self, text: str) -> str | None:
        match = re.search(r"[\w.+-]+@[\w.-]+\.\w+", text)
        if match is None:
            return None
        return match.group(0)

    def _merge_post(self, posts_by_id: dict[str, dict[str, Any]], post: dict[str, Any]) -> None:
        canonical_id = self._canonical_post_id(post.get("id"), post.get("url")) or post["id"]
        candidate = dict(post)
        candidate["id"] = canonical_id
        existing = posts_by_id.get(canonical_id)
        if existing is None:
            posts_by_id[canonical_id] = candidate
            return

        if self._post_quality_score(candidate) > self._post_quality_score(existing):
            winner = dict(candidate)
            fallback = existing
        else:
            winner = dict(existing)
            fallback = candidate

        if not winner.get("image_urls"):
            winner["image_urls"] = fallback.get("image_urls", [])
        else:
            winner["image_urls"] = self._dedupe_preserve_order(
                winner.get("image_urls", []) + fallback.get("image_urls", [])
            )

        if not winner.get("author_name"):
            winner["author_name"] = fallback.get("author_name")
        if not winner.get("published_at"):
            winner["published_at"] = fallback.get("published_at")
        if not winner.get("url") or winner.get("url") == winner.get("company_url"):
            winner["url"] = fallback.get("url") or winner.get("url")
        if winner.get("url") and winner["url"].rstrip("/") == fallback.get("url", "").rstrip("/"):
            winner["url"] = fallback.get("url") or winner["url"]

        posts_by_id[canonical_id] = winner

    def _post_quality_score(self, post: dict[str, Any]) -> tuple[int, int, int, int]:
        source_priority = {
            "guest_html": 3,
            "embedded_json": 2,
            "json_ld": 1,
        }
        return (
            source_priority.get(post.get("source", ""), 0),
            len(post.get("image_urls", [])),
            1 if post.get("published_at") else 0,
            len(post.get("text", "")),
        )

    def _sorted_posts(self, posts_by_id: dict[str, dict[str, Any]], max_posts: int) -> list[dict[str, Any]]:
        posts = list(posts_by_id.values())
        posts.sort(key=lambda post: post.get("published_at") or "", reverse=True)
        return posts[:max_posts]

    def _clean_text(self, raw_text: str) -> str:
        text = html.unescape(raw_text)
        text = TAG_PATTERN.sub(" ", text)
        text = WHITESPACE_PATTERN.sub(" ", text).strip()
        return text

    def _extract_first_match(self, text: str, pattern: str) -> str | None:
        match = re.search(pattern, text)
        if match is None:
            return None
        return self._clean_text(match.group("value"))

    def _looks_like_timestamp(self, value: str) -> bool:
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
        return True
