# Moddis Backend

Minimal Vercel-friendly FastAPI backend for Moddis Resources.

## Endpoints

- `GET /` redirects to `https://moddisresources.com`
- `POST /api/internal/linkedin-refresh`
- `GET /api/linkedin/posts`
- `GET /api/linkedin/jobs`

## Environment Variables

- `MONGODB_URL`
- `MONGODB_DB_NAME`
- `MONGODB_LINKEDIN_COLLECTION`
- `MONGODB_LINKEDIN_JOBS_COLLECTION`
- `LINKEDIN_COMPANY_POSTS_URL`
- `LINKEDIN_CACHE_TTL_SECONDS`
- `LINKEDIN_REQUEST_TIMEOUT_SECONDS`
- `LINKEDIN_REFRESH_MAX_POSTS`
- `CRON_SECRET`

## Notes

- `POST /api/internal/linkedin-refresh` returns `204 No Content` after the refresh runs.
- `vercel.json` still points the Vercel cron job at `/api/internal/linkedin-refresh`.
