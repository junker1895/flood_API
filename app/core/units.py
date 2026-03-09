from collections.abc import Callable

CONVERSIONS: dict[tuple[str, str], Callable[[float], float]] = {
    ("ft3/s", "m3/s"): lambda v: v * 0.0283168466,
    ("kcfs", "m3/s"): lambda v: v * 28.3168466,
    ("ft", "m"): lambda v: v * 0.3048,
    ("cm", "m"): lambda v: v / 100,
    ("acre-ft", "m3"): lambda v: v * 1233.48184,
    ("ML", "m3"): lambda v: v * 1000,
}

CANONICAL_UNITS = {
    "discharge": "m3/s",
    "stage": "m",
    "water_level": "m",
    "storage": "m3",
    "reservoir_storage": "m3",
    "reservoir_elevation": "m",
    "rainfall": "mm",
    "velocity": "m/s",
    "temperature": "degC",
}


def to_canonical(value: float, native_unit: str, prop: str) -> tuple[float | None, str | None]:
    canonical = CANONICAL_UNITS.get(prop)
    if not canonical:
        return None, None
    if native_unit == canonical:
        return value, canonical
    fn = CONVERSIONS.get((native_unit, canonical))
    if not fn:
        return None, None
    return fn(value), canonical
