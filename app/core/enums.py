from enum import StrEnum


class SourceType(StrEnum):
    OBSERVED = "observed"
    MODELED = "modeled"


class EntityType(StrEnum):
    STATION = "station"
    REACH = "reach"


class CanonicalProperty(StrEnum):
    DISCHARGE = "discharge"
    STAGE = "stage"
    WATER_LEVEL = "water_level"
    STORAGE = "storage"
    VELOCITY = "velocity"
    RAINFALL = "rainfall"
    TEMPERATURE = "temperature"
    SWE = "snow_water_equivalent"
    RESERVOIR_STORAGE = "reservoir_storage"
    RESERVOIR_ELEVATION = "reservoir_elevation"


class WaterbodyType(StrEnum):
    RIVER = "river"
    STREAM = "stream"
    CREEK = "creek"
    CANAL = "canal"
    RESERVOIR = "reservoir"
    LAKE = "lake"
    ESTUARY = "estuary"
    UNKNOWN = "unknown"


class AggregationType(StrEnum):
    INSTANTANEOUS = "instantaneous"
    MEAN = "mean"
    MIN = "min"
    MAX = "max"
    SUM = "sum"
    UNKNOWN = "unknown"


class QualityCode(StrEnum):
    VERIFIED = "verified"
    PROVISIONAL = "provisional"
    ESTIMATED = "estimated"
    RAW = "raw"
    MISSING = "missing"
    FLAGGED = "flagged"
    FORECAST = "forecast"
    UNKNOWN = "unknown"
