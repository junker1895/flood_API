import os
from dataclasses import dataclass
from enum import StrEnum


class JobType(StrEnum):
    METADATA = "metadata"
    LATEST = "latest"
    HISTORY = "history"
    THRESHOLDS = "thresholds"
    WARNINGS = "warnings"


@dataclass(frozen=True)
class JobSchedule:
    enabled: bool
    interval_minutes: int
    timeout_seconds: float | None = None
    max_retries: int | None = None


@dataclass(frozen=True)
class ProviderSchedule:
    provider_id: str
    enabled: bool
    supported_jobs: frozenset[JobType]
    jobs: dict[JobType, JobSchedule]


DEFAULT_PROVIDER_CAPABILITIES: dict[str, frozenset[JobType]] = {
    "usgs": frozenset({JobType.METADATA, JobType.LATEST, JobType.HISTORY}),
    "ea_england": frozenset({JobType.METADATA, JobType.LATEST, JobType.WARNINGS}),
    "geoglows": frozenset({JobType.METADATA, JobType.LATEST}),
    "whos": frozenset({JobType.METADATA}),
}

DEFAULT_PROVIDER_ENABLED: dict[str, bool] = {
    "usgs": True,
    "ea_england": True,
    "geoglows": True,
    "whos": False,
}

DEFAULT_JOB_INTERVAL_MINUTES: dict[JobType, int] = {
    JobType.METADATA: 24 * 60,
    JobType.LATEST: 10,
    JobType.HISTORY: 6 * 60,
    JobType.THRESHOLDS: 24 * 60,
    JobType.WARNINGS: 30,
}

LEGACY_PROVIDER_ENABLE_ENV: dict[str, str] = {
    "usgs": "ENABLE_PROVIDER_USGS",
    "ea_england": "ENABLE_PROVIDER_EA",
    "geoglows": "ENABLE_PROVIDER_GEOGLOWS",
    "whos": "ENABLE_PROVIDER_WHOS",
}

LEGACY_PROVIDER_LATEST_INTERVAL_ENV: dict[str, str] = {
    "usgs": "USGS_POLL_MINUTES",
    "ea_england": "EA_POLL_MINUTES",
    "geoglows": "GEOGLOWS_POLL_MINUTES",
}


def _provider_env_key(provider_id: str) -> str:
    return provider_id.upper()


def _job_env_key(job_type: JobType) -> str:
    return job_type.value.upper()


def _parse_bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(raw: str | None, default: int) -> int:
    if raw is None:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _parse_float(raw: str | None) -> float | None:
    if raw is None:
        return None
    try:
        parsed = float(raw)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _provider_enabled(provider_id: str) -> bool:
    provider_key = _provider_env_key(provider_id)
    legacy_env = LEGACY_PROVIDER_ENABLE_ENV.get(provider_id)
    if legacy_env and legacy_env in os.environ:
        return _parse_bool(os.getenv(legacy_env), DEFAULT_PROVIDER_ENABLED.get(provider_id, True))
    return _parse_bool(
        os.getenv(f"PROVIDERS__{provider_key}__ENABLED"),
        DEFAULT_PROVIDER_ENABLED.get(provider_id, True),
    )


def _job_enabled(provider_id: str, job_type: JobType, supported_jobs: frozenset[JobType]) -> bool:
    if job_type not in supported_jobs:
        return False
    provider_key = _provider_env_key(provider_id)
    job_key = _job_env_key(job_type)
    return _parse_bool(os.getenv(f"PROVIDERS__{provider_key}__JOBS__{job_key}__ENABLED"), True)


def _job_interval_minutes(provider_id: str, job_type: JobType) -> int:
    provider_key = _provider_env_key(provider_id)
    job_key = _job_env_key(job_type)
    default = DEFAULT_JOB_INTERVAL_MINUTES[job_type]

    if job_type == JobType.LATEST:
        legacy_poll_env = LEGACY_PROVIDER_LATEST_INTERVAL_ENV.get(provider_id)
        if legacy_poll_env and legacy_poll_env in os.environ:
            default = _parse_int(os.getenv(legacy_poll_env), default)

    return _parse_int(
        os.getenv(f"PROVIDERS__{provider_key}__JOBS__{job_key}__INTERVAL_MINUTES"),
        default,
    )


def _job_timeout_seconds(provider_id: str, job_type: JobType) -> float | None:
    provider_key = _provider_env_key(provider_id)
    job_key = _job_env_key(job_type)
    return _parse_float(os.getenv(f"PROVIDERS__{provider_key}__JOBS__{job_key}__TIMEOUT_SECONDS"))


def _job_max_retries(provider_id: str, job_type: JobType) -> int | None:
    provider_key = _provider_env_key(provider_id)
    job_key = _job_env_key(job_type)
    value = os.getenv(f"PROVIDERS__{provider_key}__JOBS__{job_key}__MAX_RETRIES")
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def build_provider_schedule(provider_id: str) -> ProviderSchedule:
    supported_jobs = DEFAULT_PROVIDER_CAPABILITIES.get(provider_id, frozenset())
    enabled = _provider_enabled(provider_id)
    jobs: dict[JobType, JobSchedule] = {}
    for job_type in JobType:
        jobs[job_type] = JobSchedule(
            enabled=_job_enabled(provider_id, job_type, supported_jobs),
            interval_minutes=_job_interval_minutes(provider_id, job_type),
            timeout_seconds=_job_timeout_seconds(provider_id, job_type),
            max_retries=_job_max_retries(provider_id, job_type),
        )

    return ProviderSchedule(
        provider_id=provider_id,
        enabled=enabled,
        supported_jobs=supported_jobs,
        jobs=jobs,
    )


def get_enabled_provider_jobs(provider_ids: list[str] | None = None) -> list[tuple[str, JobType, JobSchedule]]:
    selected_provider_ids = provider_ids or list(DEFAULT_PROVIDER_CAPABILITIES.keys())
    provider_jobs: list[tuple[str, JobType, JobSchedule]] = []
    for provider_id in selected_provider_ids:
        schedule = build_provider_schedule(provider_id)
        if not schedule.enabled:
            continue
        for job_type in JobType:
            job_schedule = schedule.jobs[job_type]
            if not job_schedule.enabled:
                continue
            provider_jobs.append((provider_id, job_type, job_schedule))
    return provider_jobs
