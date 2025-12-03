from .apple_music_scraper import run_apple_music_scraper
from .disney_plus_scraper import run_disney_scraper

DSP_OPTIONS = {
    "Apple Music": "apple",
    "Disney+": "disney",
    # later you can add Spotify, Netflix, etc.
}

def run_scraper(dsp_name: str, test_mode: bool):
    """
    Unified function the web app will call.

    dsp_name: 'Apple Music' or 'Disney+'
    test_mode: True  -> test / quick mode
               False -> full mode
    Returns: path to Excel file created.
    """
    kind = DSP_OPTIONS.get(dsp_name)
    if kind is None:
        raise ValueError(f"Unknown DSP: {dsp_name}")

    if kind == "apple":
        return run_apple_music_scraper(test_mode=test_mode)

    if kind == "disney":
        # Disney uses 'MODE' = 'test'/'full' instead of a boolean
        mode = "test" if test_mode else "full"
        return run_disney_scraper(mode=mode)
