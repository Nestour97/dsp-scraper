# dsp_scrapers/__init__.py

from .apple_music_scraper import run_apple_music_scraper
from .disney_plus_scraper import run_disney_scraper
from .icloud_plus_scraper import run_icloud_plus_scraper
from .spotify_scraper import run_spotify_scraper
from .netflix_scraper import run_netflix_scraper

# Leaf scrapers (what the app can actually run)
DSP_OPTIONS = {
    "Apple Music": "apple_music",
    "iCloud+": "icloud_plus",
    "Spotify": "spotify",
    "Netflix": "netflix",
    "Disney+": "disney",
}

# High-level groups for the UI (so iCloud+ sits under Apple)
DSP_GROUPS = {
    "Apple": ["Apple Music", "iCloud+"],
    "Spotify": ["Spotify"],
    "Netflix": ["Netflix"],
    "Disney+": ["Disney+"],
}


def run_scraper(dsp_name: str, test_mode: bool):
    """
    Unified entry point that the Streamlit app calls.

    dsp_name: one of DSP_OPTIONS.keys()
    test_mode: True  -> quick / sample run
               False -> full run (all countries, where supported)
    Returns: absolute path to the Excel file that was created.
    """
    kind = DSP_OPTIONS.get(dsp_name)
    if kind is None:
        raise ValueError(f"Unknown DSP: {dsp_name}")

    if kind == "apple_music":
        return run_apple_music_scraper(test_mode=test_mode)

    if kind == "icloud_plus":
        return run_icloud_plus_scraper(test_mode=test_mode)

    if kind == "spotify":
        return run_spotify_scraper(test_mode=test_mode)

    if kind == "netflix":
        return run_netflix_scraper(test_mode=test_mode)

    if kind == "disney":
        mode = "test" if test_mode else "full"
        return run_disney_scraper(mode=mode)

    raise ValueError(f"No handler implemented for kind '{kind}'")
