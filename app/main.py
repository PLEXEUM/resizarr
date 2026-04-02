import asyncio
import secrets
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from app.db.database import init_db, get_connection
from app.core.scheduler import start_scheduler, stop_scheduler
from app.core.poller import start_poller
from app.utils.logger import setup_logger, get_logger

# --- Startup & Shutdown ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup and shutdown events."""
    # Initialize database
    init_db()

    # Setup logger with settings from DB
    conn = get_connection()
    settings = conn.execute("SELECT * FROM settings WHERE id = 1").fetchone()

    if not settings:
        # Insert default settings on first run
        conn.execute("""
            INSERT INTO settings (id, batch_size, cron_schedule, poller_interval,
                log_level, log_max_size_mb, log_max_files)
            VALUES (1, 10, '0 2 * * *', 5, 'INFO', 10, 5)
        """)
        conn.commit()
        settings = conn.execute("SELECT * FROM settings WHERE id = 1").fetchone()

    setup_logger(
        log_level=settings["log_level"],
        log_max_size_mb=settings["log_max_size_mb"],
        log_max_files=settings["log_max_files"]
    )
    logger = get_logger()

    # Generate API key on first startup
    config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
    if not config:
        api_key = secrets.token_hex(32)
        conn.execute("""
            INSERT INTO config (id, api_key)
            VALUES (1, ?)
        """, (api_key,))
        conn.commit()
        logger.info(f"Generated new API key on first startup")
    conn.close()

    # Start scheduler
    logger.info("Starting scheduler...")
    start_scheduler(settings["cron_schedule"])

    # Start poller as background task
    logger.info("Starting poller...")
    poller_task = asyncio.create_task(
        start_poller(settings["poller_interval"])
    )

    logger.info("Resizarr started successfully on port 7227")

    yield  # App is running

    # Shutdown
    logger.info("Shutting down Resizarr...")
    poller_task.cancel()
    stop_scheduler()


# --- App Setup ---
app = FastAPI(
    title="Resizarr",
    description="Automated media replacement tool for Radarr",
    version="1.0.0",
    lifespan=lifespan
)

# Static files and templates
app.mount("/static", StaticFiles(directory="app/web/static"), name="static")
templates = Jinja2Templates(directory="app/web/templates")

# --- Health Check ---
@app.get("/health")
async def health():
    return {"status": "ok"}

# --- Import and register API routers ---
from app.api import config, rules, settings, runs, pending, logs, backup

app.include_router(config.router,   prefix="/api")
app.include_router(rules.router,    prefix="/api")
app.include_router(settings.router, prefix="/api")
app.include_router(runs.router,     prefix="/api")
app.include_router(pending.router,  prefix="/api")
app.include_router(logs.router,     prefix="/api")
app.include_router(backup.router,   prefix="/api")

# --- Frontend Routes ---
from fastapi import Request
from fastapi.responses import RedirectResponse

@app.get("/")
async def root(request: Request):
    """Redirect to setup if not configured, otherwise dashboard."""
    conn = get_connection()
    config_row = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
    conn.close()

    if not config_row or not config_row["radarr_url"]:
        return RedirectResponse(url="/setup")

    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/setup")
async def setup_page(request: Request):
    return templates.TemplateResponse("setup.html", {"request": request})

@app.get("/rules")
async def rules_page(request: Request):
    return templates.TemplateResponse("rules.html", {"request": request})

@app.get("/settings")
async def settings_page(request: Request):
    return templates.TemplateResponse("settings.html", {"request": request})

@app.get("/pending")
async def pending_page(request: Request):
    return templates.TemplateResponse("pending.html", {"request": request})

@app.get("/logs")
async def logs_page(request: Request):
    return templates.TemplateResponse("logs.html", {"request": request})