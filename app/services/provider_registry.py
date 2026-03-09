from app.core.time import utcnow
from app.db.models import Provider


PROVIDER_DEFINITIONS = {
    "usgs": {
        "name": "USGS Water Services",
        "provider_type": "government",
        "home_url": "https://www.usgs.gov/",
        "api_base_url": "https://waterservices.usgs.gov/nwis/",
        "license_name": "US Public Domain",
        "status": "active",
    },
    "ea_england": {
        "name": "Environment Agency Flood Monitoring (England)",
        "provider_type": "government",
        "home_url": "https://environment.data.gov.uk/flood-monitoring/",
        "api_base_url": "https://environment.data.gov.uk/flood-monitoring/id/",
        "license_name": "Open Government Licence",
        "status": "active",
    },
    "geoglows": {
        "name": "GEOGLOWS",
        "provider_type": "modeled",
        "home_url": "https://geoglows.org/",
        "api_base_url": "https://geoglows.ecmwf.int/",
        "license_name": "Provider Terms",
        "status": "active",
    },
    "whos": {
        "name": "WMO WHOS",
        "provider_type": "metadata",
        "home_url": "https://public.wmo.int/en/programmes/world-hydrological-status-and-outlook-system-whos",
        "api_base_url": None,
        "license_name": None,
        "status": "inactive",
    },
}


def build_provider(provider_id: str) -> Provider:
    d = PROVIDER_DEFINITIONS[provider_id]
    now = utcnow()
    return Provider(
        provider_id=provider_id,
        name=d["name"],
        provider_type=d["provider_type"],
        home_url=d.get("home_url"),
        api_base_url=d.get("api_base_url"),
        license_name=d.get("license_name"),
        license_url=None,
        attribution_text=None,
        default_poll_interval_minutes=None,
        status=d.get("status", "active"),
        auth_type=None,
        created_at=now,
        updated_at=now,
    )
