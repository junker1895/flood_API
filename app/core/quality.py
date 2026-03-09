from app.core.enums import QualityCode


def normalize_quality(raw: str | None, forecast: bool = False) -> dict[str, bool | str]:
    key = (raw or "").lower()
    code = QualityCode.UNKNOWN
    if forecast:
        code = QualityCode.FORECAST
    elif "prov" in key:
        code = QualityCode.PROVISIONAL
    elif "estim" in key:
        code = QualityCode.ESTIMATED
    elif "ver" in key:
        code = QualityCode.VERIFIED
    elif "miss" in key:
        code = QualityCode.MISSING
    elif "flag" in key:
        code = QualityCode.FLAGGED
    elif key:
        code = QualityCode.RAW
    return {
        "quality_code": code.value,
        "is_provisional": code == QualityCode.PROVISIONAL,
        "is_estimated": code == QualityCode.ESTIMATED,
        "is_missing": code == QualityCode.MISSING,
        "is_forecast": code == QualityCode.FORECAST,
        "is_flagged": code == QualityCode.FLAGGED,
    }
