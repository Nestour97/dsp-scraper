from typing import Iterable, List, Optional

from .apple_music_scraper import run_apple_music_scraper
from .disney_plus_scraper import run_disney_scraper

DSP_OPTIONS = {
    "Apple Music": "apple",
    "Disney+": "disney",
    # later you can add Spotify, Netflix, etc.
}


def run_scraper(
    dsp_name: str,
    test_mode: bool,
    country_codes: Optional[Iterable[str]] = None,
):
    """
    Unified function the web app will call.

    dsp_name: 'Apple Music' or 'Disney+'
    test_mode: True  -> test / quick mode
               False -> full mode
    country_codes: optional iterable of ISO-2 country codes for subset runs.
                   Currently used by Apple Music only.
    Returns: path to Excel file created.
    """
    kind = DSP_OPTIONS.get(dsp_name)
    if kind is None:
        raise ValueError(f"Unknown DSP: {dsp_name}")

    # Normalise codes to a simple list of two-letter strings
    codes: List[str] = []
    if country_codes:
        for c in country_codes:
            if not c:
                continue
            cc = str(c).strip().upper()
            if len(cc) == 2:
                codes.append(cc)

    if kind == "apple":
        # Apple scraper accepts ISO-2 codes directly
        return run_apple_music_scraper(
            test_mode=test_mode,
            country_codes=codes or None,
        )

    if kind == "disney":
        # Disney currently ignores custom countries and just uses its own
        # 'test' or 'full' list internally.
        mode = "test" if test_mode else "full"
        return run_disney_scraper(mode=mode)

    raise ValueError(f"Unsupported DSP kind: {kind}")
