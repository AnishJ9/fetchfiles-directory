"""City-name-to-metro mapping for chains whose pages lack lat/lng.

Used as a fallback filter when we cannot geocode to the metro bbox directly.
Centroid lat/lng is used for geocoding when only address text is available.
"""

# (city_lower, state_2letter) -> (metro, approx_lat, approx_lng)
METRO_CITY_CENTROIDS: dict[tuple[str, str], tuple[str, float, float]] = {
    # Atlanta metro (GA)
    ("atlanta", "GA"):          ("atlanta", 33.7490, -84.3880),
    ("sandy springs", "GA"):    ("atlanta", 33.9304, -84.3733),
    ("roswell", "GA"):          ("atlanta", 34.0232, -84.3616),
    ("alpharetta", "GA"):       ("atlanta", 34.0754, -84.2941),
    ("marietta", "GA"):         ("atlanta", 33.9526, -84.5499),
    ("decatur", "GA"):          ("atlanta", 33.7748, -84.2963),
    ("duluth", "GA"):           ("atlanta", 34.0029, -84.1446),
    ("smyrna", "GA"):           ("atlanta", 33.8839, -84.5144),
    ("dunwoody", "GA"):         ("atlanta", 33.9462, -84.3346),
    ("johns creek", "GA"):      ("atlanta", 34.0289, -84.1986),
    ("peachtree corners", "GA"):("atlanta", 33.9700, -84.2216),
    ("kennesaw", "GA"):         ("atlanta", 34.0234, -84.6155),
    ("lawrenceville", "GA"):    ("atlanta", 33.9562, -83.9880),
    ("snellville", "GA"):       ("atlanta", 33.8573, -84.0199),
    ("brookhaven", "GA"):       ("atlanta", 33.8651, -84.3365),
    ("cumming", "GA"):          ("atlanta", 34.2073, -84.1402),
    ("college park", "GA"):     ("atlanta", 33.6534, -84.4494),
    ("douglasville", "GA"):     ("atlanta", 33.7515, -84.7477),
    ("acworth", "GA"):          ("atlanta", 34.0661, -84.6777),
    ("powder springs", "GA"):   ("atlanta", 33.8595, -84.6838),
    ("mcdonough", "GA"):        ("atlanta", 33.4473, -84.1469),
    ("stockbridge", "GA"):      ("atlanta", 33.5443, -84.2338),
    ("fayetteville", "GA"):     ("atlanta", 33.4487, -84.4547),
    ("morrow", "GA"):           ("atlanta", 33.5834, -84.3393),
    ("austell", "GA"):          ("atlanta", 33.8132, -84.6352),
    ("tucker", "GA"):           ("atlanta", 33.8545, -84.2171),
    ("stone mountain", "GA"):   ("atlanta", 33.8081, -84.1702),
    ("union city", "GA"):       ("atlanta", 33.5871, -84.5422),
    ("east point", "GA"):       ("atlanta", 33.6793, -84.4394),
    ("lilburn", "GA"):          ("atlanta", 33.8901, -84.1430),
    ("norcross", "GA"):         ("atlanta", 33.9412, -84.2135),
    ("doraville", "GA"):        ("atlanta", 33.8973, -84.2707),
    ("buford", "GA"):           ("atlanta", 34.1206, -83.9882),
    ("suwanee", "GA"):          ("atlanta", 34.0515, -84.0714),
    ("mableton", "GA"):         ("atlanta", 33.8187, -84.5777),
    ("hiram", "GA"):            ("atlanta", 33.8754, -84.7591),
    ("canton", "GA"):           ("atlanta", 34.2368, -84.4908),
    ("woodstock", "GA"):        ("atlanta", 34.1015, -84.5194),
    ("milton", "GA"):           ("atlanta", 34.1320, -84.3005),
    ("lawrenceville-ga", "GA"): ("atlanta", 33.9562, -83.9880),  # CBW slug variant
    ("warner robins", "GA"):    (None, 0, 0),  # NOT in metro bbox
    # Tampa metro (FL)
    ("tampa", "FL"):            ("tampa", 27.9506, -82.4572),
    ("st. petersburg", "FL"):   ("tampa", 27.7676, -82.6403),
    ("st petersburg", "FL"):    ("tampa", 27.7676, -82.6403),
    ("saint petersburg", "FL"): ("tampa", 27.7676, -82.6403),
    ("clearwater", "FL"):       ("tampa", 27.9659, -82.8001),
    ("brandon", "FL"):          ("tampa", 27.9378, -82.2859),
    ("largo", "FL"):            ("tampa", 27.9095, -82.7873),
    ("riverview", "FL"):        ("tampa", 27.8659, -82.3265),
    ("wesley chapel", "FL"):    ("tampa", 28.2397, -82.3275),
    ("temple terrace", "FL"):   ("tampa", 28.0353, -82.3895),
    ("carrollwood", "FL"):      ("tampa", 28.0506, -82.5021),
    ("lutz", "FL"):             ("tampa", 28.1506, -82.4617),
    ("ruskin", "FL"):           ("tampa", 27.7203, -82.4326),
    ("apollo beach", "FL"):     ("tampa", 27.7700, -82.4020),
    ("land o lakes", "FL"):     ("tampa", 28.2189, -82.4615),
    ("plant city", "FL"):       ("tampa", 28.0189, -82.1242),
    ("valrico", "FL"):          ("tampa", 27.9417, -82.2379),
    ("pinellas park", "FL"):    ("tampa", 27.8428, -82.6995),
    ("seminole", "FL"):         ("tampa", 27.8395, -82.7901),
    ("palm harbor", "FL"):      ("tampa", 28.0780, -82.7637),
    ("dunedin", "FL"):          ("tampa", 28.0198, -82.7873),
    ("tarpon springs", "FL"):   ("tampa", 28.1461, -82.7565),
    ("oldsmar", "FL"):          ("tampa", 28.0342, -82.6651),
    ("safety harbor", "FL"):    ("tampa", 27.9903, -82.6929),
    # Austin metro (TX)
    ("austin", "TX"):           ("austin", 30.2672, -97.7431),
    ("round rock", "TX"):       ("austin", 30.5083, -97.6789),
    ("cedar park", "TX"):       ("austin", 30.5052, -97.8203),
    ("pflugerville", "TX"):     ("austin", 30.4394, -97.6200),
    ("georgetown", "TX"):       ("austin", 30.6333, -97.6780),
    ("leander", "TX"):          ("austin", 30.5788, -97.8531),
    ("kyle", "TX"):             ("austin", 29.9893, -97.8772),
    ("buda", "TX"):             ("austin", 30.0852, -97.8406),
    ("bee cave", "TX"):         ("austin", 30.3085, -97.9481),
    ("lakeway", "TX"):          ("austin", 30.3641, -97.9753),
    ("lake travis", "TX"):      ("austin", 30.3641, -97.9753),
    ("manor", "TX"):            ("austin", 30.3404, -97.5569),
    ("san marcos", "TX"):       (None, 0, 0),  # Outside bbox (<30.00)
    ("dripping springs", "TX"): ("austin", 30.1902, -98.0867),
    # Nashville metro (TN)
    ("nashville", "TN"):        ("nashville", 36.1627, -86.7816),
    ("franklin", "TN"):         ("nashville", 35.9251, -86.8689),
    ("brentwood", "TN"):        ("nashville", 35.9973, -86.7828),
    ("murfreesboro", "TN"):     (None, 0, 0),  # Outside bbox (<35.80)
    ("hendersonville", "TN"):   ("nashville", 36.3047, -86.6200),
    ("mt juliet", "TN"):        ("nashville", 36.1998, -86.5186),
    ("mount juliet", "TN"):     ("nashville", 36.1998, -86.5186),
    ("mt. juliet", "TN"):       ("nashville", 36.1998, -86.5186),
    ("gallatin", "TN"):         ("nashville", 36.3881, -86.4467),
    ("smyrna", "TN"):           (None, 0, 0),  # Outside bbox (<35.80)
    ("la vergne", "TN"):        (None, 0, 0),  # Outside bbox (<35.80)
    ("lavergne", "TN"):         (None, 0, 0),
    ("antioch", "TN"):          ("nashville", 36.0592, -86.6717),
    ("goodlettsville", "TN"):   ("nashville", 36.3231, -86.7133),
    ("nolensville", "TN"):      ("nashville", 35.9534, -86.6691),
    ("spring hill", "TN"):      (None, 0, 0),
    # Asheville metro (NC)
    ("asheville", "NC"):        ("asheville", 35.5951, -82.5515),
    ("arden", "NC"):            ("asheville", 35.4767, -82.5165),
    ("fletcher", "NC"):         ("asheville", 35.4314, -82.5001),
    ("hendersonville", "NC"):   (None, 0, 0),  # Outside bbox (<35.35)
    ("black mountain", "NC"):   ("asheville", 35.6174, -82.3214),
    ("weaverville", "NC"):      ("asheville", 35.6968, -82.5610),
    ("candler", "NC"):          ("asheville", 35.5383, -82.6959),
    ("swannanoa", "NC"):        ("asheville", 35.6009, -82.4001),
}


def metro_for_city_state(city: str, state: str) -> tuple[str | None, float, float]:
    """Return (metro, approx_lat, approx_lng) for a city/state pair, or (None, 0, 0)."""
    if not city or not state:
        return None, 0, 0
    key = (city.strip().lower(), state.strip().upper())
    hit = METRO_CITY_CENTROIDS.get(key)
    if hit and hit[0]:
        return hit
    return None, 0, 0
