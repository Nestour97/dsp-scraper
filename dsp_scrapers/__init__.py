from .apple_music_scraper import run_apple_music_scraper
from .disney_plus_scraper import run_disney_scraper
from .icloud_plus_scraper import run_icloud_plus_scraper
from .spotify_scraper import run_spotify_scraper
from .netflix_scraper import run_netflix_scraper

# Map the names used in the UI to internal scraper "kinds"
DSP_OPTIONS = {
    "Apple Music": "apple_music",
    "iCloud+": "icloud_plus",
    "Spotify": "spotify",
    "Netflix": "netflix",
    "Disney+": "disney",
}


def run_scraper(dsp_name: str, test_mode: bool):
    """
    Unified function the Streamlit app calls.

    Parameters
    ----------
    dsp_name : str
        One of: "Apple Music", "iCloud+", "Spotify", "Netflix", "Disney+".
    test_mode : bool
        True  -> "Test (quick run)"
        False -> "Full (all countries)"

    Returns
    -------
    str
        Path to the Excel file created.
    """
    kind = DSP_OPTIONS.get(dsp_name)
    if kind is None:
        raise ValueError(f"Unknown DSP: {dsp_name}")

    # ---- Apple Music ----
    if kind == "apple_music":
        # Wrapper already flips TEST_MODE internally
        return run_apple_music_scraper(test_mode=test_mode)

    # ---- iCloud+ ----
    if kind == "icloud_plus":
        return run_icloud_plus_scraper(test_mode=test_mode)

    # ---- Spotify ----
    if kind == "spotify":
        return run_spotify_scraper(test_mode=test_mode)

    # ---- Netflix ----
    if kind == "netflix":
        return run_netflix_scraper(test_mode=test_mode)

    # ---- Disney+ ----
    if kind == "disney":
        # Disney wrapper expects mode="test" or "full"
        mode = "test" if test_mode else "full"
        return run_disney_scraper(mode=mode)

    # Should never reach here
    raise ValueError(f"No handler implemented for kind '{kind}'")
