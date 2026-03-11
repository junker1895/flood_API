from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import forecast, health, providers, reaches, stations, thresholds, warnings
from app.core.config import settings
from app.core.logging import configure_logging

configure_logging(settings.log_level)
app = FastAPI(title=settings.app_name)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "https://junker1895.github.io",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(providers.router, prefix=settings.api_prefix)
app.include_router(stations.router, prefix=settings.api_prefix)
app.include_router(reaches.router, prefix=settings.api_prefix)
app.include_router(warnings.router, prefix=settings.api_prefix)
app.include_router(thresholds.router, prefix=settings.api_prefix)
app.include_router(forecast.router, prefix=settings.api_prefix)
app.include_router(health.router)
