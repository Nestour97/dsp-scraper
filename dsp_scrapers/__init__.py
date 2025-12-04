# dsp_scrapers/__init__.py

from typing import Iterable, Optional

from .apple_music_scraper import run_apple_music_scraper
from .disney_plus_scraper import run_disney_scraper
from .spotify_scraper import run_spotify_scraper
from .icloud_plus_scraper import run_icloud_plus_scraper
from .netflix_scraper import run_netflix_scraper


DSP_OPTIONS = {
    "Apple Music": "apple_music",
    "iCloud+": "icloud_plus",
    "Spotify": "spotify",
    "Netflix": "netflix",
    "Disney+": "disney",
}


def run_scraper(
    dsp_name: str,
    test_mode: bool,
    test_countries: Optional[Iterable[str]] = None,
) -> str:
    """
    Unified function called by the Streamlit app.

    dsp_name:
        One of: 'Apple Music', 'iCloud+', 'Spotify', 'Netflix', 'Disney+'.
    test_mode:
        True  -> quick / test run.
        False -> full global run.
    test_countries:
        Optional iterable of ISO-2 country codes (e.g. ['GB', 'US']).
        Some scrapers (Netflix, Spotify) use this. Others ignore it.
    """

    kind = DSP_OPTIONS.get(dsp_name)
    if kind is None:
        raise ValueError(f"Unknown DSP: {dsp_name}")

    if kind == "apple_music":
        # Your Apple Music scraper already knows how to handle test_mode.
        return run_apple_music_scraper(test_mode=test_mode)

    if kind == "icloud_plus":
        # iCloud+ currently uses its own internal test country list.
        # If you later want to use test_countries, wire them into that function.
        return run_icloud_plus_scraper(test_mode=test_mode)

    if kind == "spotify":
        # Spotify scraper was already adapted to accept test_countries.
        return run_spotify_scraper(test_mode=test_mode, test_countries=test_countries)

    if kind == "netflix":
        # New Netflix wrapper that always respects test_countries in test mode.
        return run_netflix_scraper(test_mode=test_mode, test_countries=test_countries)

    if kind == "disney":
        mode = "test" if test_mode else "full"
        return run_disney_scraper(mode=mode)

    raise RuntimeError("Unreachable code in run_scraper")
