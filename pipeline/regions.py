from __future__ import annotations

SAN_DIEGO_ZIP_PREFIXES = ("919", "920", "921")
BAY_AREA_ZIP_PREFIXES = (
    "940",
    "941",
    "943",
    "944",
    "945",
    "946",
    "947",
    "948",
    "949",
    "950",
    "951",
    "954",
)

SAN_DIEGO_COUNTIES = ("SAN DIEGO",)
BAY_AREA_COUNTIES = (
    "ALAMEDA",
    "CONTRA COSTA",
    "MARIN",
    "NAPA",
    "SAN FRANCISCO",
    "SAN MATEO",
    "SANTA CLARA",
    "SOLANO",
    "SONOMA",
)


def normalize_zip5(value: str | None) -> str:
    if not value:
        return ""
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits[:5]


def region_label_from_zip(value: str | None) -> str:
    zip5 = normalize_zip5(value)
    if not zip5:
        return ""
    for prefix in SAN_DIEGO_ZIP_PREFIXES:
        if zip5.startswith(prefix):
            return "San Diego"
    for prefix in BAY_AREA_ZIP_PREFIXES:
        if zip5.startswith(prefix):
            return "Bay Area"
    return ""
