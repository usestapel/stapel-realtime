"""The library's CONFIG.MD must parse with stapel-core's manifest parser."""
from pathlib import Path

from stapel_core.config import parse_config_md

_CONFIG_MD = Path(__file__).resolve().parent.parent / "CONFIG.MD"


def test_config_md_parses_with_core():
    entries = parse_config_md(_CONFIG_MD)
    assert "URL_PREFIX" in entries
    assert entries["URL_PREFIX"].source == "settings"
