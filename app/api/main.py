from fastapi import FastAPI

from app.api.routes import health, providers, reaches, stations, warnings
from app.core.config import settings
from app.core.logging import configure_logging

configure_logging(settings.log_level)
app = FastAPI(title=settings.app_name)

app.include_router(providers.router, prefix=settings.api_prefix)
app.include_router(stations.router, prefix=settings.api_prefix)
app.include_router(reaches.router, prefix=settings.api_prefix)
app.include_router(warnings.router, prefix=settings.api_prefix)
app.include_router(health.router)
