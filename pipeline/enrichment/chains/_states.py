"""State utilities."""

STATE_TWO = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new-hampshire": "NH", "new-jersey": "NJ", "new-mexico": "NM", "new-york": "NY",
    "north-carolina": "NC", "north-dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode-island": "RI",
    "south-carolina": "SC", "south-dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west-virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}

# Target states map (lower-case slug + two-letter)
TARGET_STATES_TWO = {"GA", "FL", "TX", "TN", "NC"}

# States to slugs for chains using long-form
TARGET_STATES_LONG = {"georgia", "florida", "texas", "tennessee", "north-carolina"}


def two_letter(name_or_code: str) -> str:
    if not name_or_code:
        return ""
    s = name_or_code.strip().lower().replace(" ", "-")
    if len(s) == 2:
        return s.upper()
    return STATE_TWO.get(s, name_or_code.strip().upper() if len(name_or_code) == 2 else "")
