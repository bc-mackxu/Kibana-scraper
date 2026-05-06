#!/usr/bin/env python3
"""Kibana Scraper Web App — run: python3 app.py"""
import threading
import webbrowser
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from db import init_db
from routers._run_engine import scheduler, _refresh_scheduler, SCRAPER_DIR
from routers._filters import _build_log_filter  # noqa: F401 — re-exported for compatibility
from routers import jobs, collections, groups, logs, analysis, classifiers as classifiers_router
from routers import settings as settings_router
from routers.settings import _load_keys

STATIC_DIR = SCRAPER_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)

_load_keys()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    _refresh_scheduler()
    if not scheduler.running:
        scheduler.start()
    print(f"SQLite DB: {__import__('db').DB_PATH}")
    print("Scheduler started")
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Kibana Scraper", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8765", "http://127.0.0.1:8765"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/static/{filename}")
def static_file(filename: str):
    p = STATIC_DIR / filename
    # Path traversal guard: reject any path that escapes the static dir
    if not p.resolve().is_relative_to(STATIC_DIR.resolve()):
        raise HTTPException(403, "Forbidden")
    if not p.exists():
        raise HTTPException(404, "Not found")
    return FileResponse(str(p))


app.include_router(jobs.router)
app.include_router(collections.router)
app.include_router(groups.router)
app.include_router(logs.router)
app.include_router(analysis.router)
app.include_router(settings_router.router)
app.include_router(classifiers_router.router)


if __name__ == "__main__":
    import uvicorn
    threading.Timer(1.2, lambda: webbrowser.open("http://localhost:8765")).start()
    uvicorn.run(app, host="0.0.0.0", port=8765)
