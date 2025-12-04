# dsp_scrapers/__init__.py

from .apple_music_scraper import run_apple_music_scraper
from .disney_plus_scraper import run_disney_scraper
from .spotify_scraper import run_spotify_scraper
from .icloud_plus_scraper import run_icloud_plus_scraper  # adjust name if different
from .netflix_scraper import run_netflix_scraper         # adjust name if you wrapped it

DSP_OPTIONS = {
    "Apple Music": "apple_music",
    "iCloud+": "icloud_plus",
    "Spotify": "spotify",
    "Netflix": "netflix",
    "Disney+": "disney",
}

def run_scraper(dsp_name: str, test_mode: bool, test_countries=None) -> str:
    """
    Unified entry point used by the Streamlit app.

    dsp_name: label as used in the UI tabs
    test_mode: True = quick run, False = full run
    test_countries: optional list of ISO alpha-2 codes in Test mode
    Returns: path to the Excel file produced.
    """
    kind = DSP_OPTIONS.get(dsp_name)
    if kind is None:
        raise ValueError(f"Unknown DSP: {dsp_name}")

    if kind == "apple_music":
        # Apple Music supports per-country test runs
        return run_apple_music_scraper(
            test_mode=test_mode,
            test_countries=test_countries,
        )

    if kind == "icloud_plus":
        # Currently ignores test_countries – you can extend later if you like
        return run_icloud_plus_scraper(test_mode=test_mode)

    if kind == "spotify":
        # Spotify will also use test_countries as TEST_MARKETS (see below)
        return run_spotify_scraper(
            test_mode=test_mode,
            test_countries=test_countries,
        )

if kind == "netflix":
    return run_netflix_scraper(
        test_mode=test_mode,
        test_countries=test_countries,
    )


    if kind == "disney":
        mode = "test" if test_mode else "full"
        return run_disney_scraper(mode=mode)
