"""The contract artifacts, and the drift gate that keeps them honest.

This L1 library emits three of the five contract documents — `capabilities.json`
(hand-authored apart from module/version and the derived `surface`), `llms.txt`,
and the assembled `README.md`. There is deliberately no `flows.json` or
`errors.json`: a library with no HTTP surface and no registered error keys has
nothing to put in them, and an empty artifact reads in a catalog exactly like a
complete one — the failure mode the whole pipeline exists to prevent.

Regenerate after any change to a public function, an axis, or `docs/readme.md`:

    make contract      # then commit docs/* + README.md

The emitters are run in a **subprocess**: this process already configured Django
on the test settings, and a clean interpreter is the honest way to exercise
exactly what `make contract` runs.
"""
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

pytest.importorskip(
    "stapel_tools", reason="the contract emitters live in stapel-tools"
)


def run(*args):
    return subprocess.run(
        [sys.executable, "-m", *args], cwd=REPO, capture_output=True, text=True
    )


class TestArtifactsExist:
    def test_the_three_artifacts_are_committed(self):
        for name in ("docs/capabilities.json", "docs/llms.txt", "README.md"):
            assert (REPO / name).exists(), f"{name} is missing"

    def test_the_version_matches_pyproject(self):
        """A capabilities.json whose version lags the package is tracker #226."""
        capabilities = json.loads((REPO / "docs" / "capabilities.json").read_text())
        pyproject = (REPO / "pyproject.toml").read_text()
        declared = next(
            line.split("=")[1].strip().strip('"')
            for line in pyproject.splitlines()
            if line.startswith("version =")
        )
        assert capabilities["version"] == declared

    def test_the_wire_envelope_schema_is_committed(self):
        """The L1 exception: this contract is shared with a browser client."""
        schema = json.loads(
            (REPO / "schemas" / "wire" / "envelope.v1.json").read_text()
        )
        assert schema["properties"]["v"]["const"] == 1

    def test_every_axis_matches_a_real_setting(self):
        """A documented axis nobody reads is worse than an undocumented one.

        ``NON_APPSETTINGS_AXES`` is the one deliberate exception: URL_PREFIX
        is real and documented, but reads through
        ``stapel_realtime.conf.url_prefix()`` rather than
        ``realtime_settings``, precisely so it does NOT inherit
        ``AppSettings``'s flat-setting fallback (realtime.W004/W007).
        """
        from stapel_realtime.conf import NON_APPSETTINGS_AXES, realtime_settings

        capabilities = json.loads((REPO / "docs" / "capabilities.json").read_text())
        documented = {axis["key"] for axis in capabilities["axes"]}
        assert documented == set(realtime_settings.defaults) | NON_APPSETTINGS_AXES

    def test_config_md_documents_every_axis(self):
        config_md = (REPO / "CONFIG.MD").read_text()
        from stapel_realtime.conf import NON_APPSETTINGS_AXES, realtime_settings

        for key in set(realtime_settings.defaults) | NON_APPSETTINGS_AXES:
            assert f"| {key} |" in config_md, f"{key} is missing from CONFIG.MD"


class TestDriftGate:
    def test_capabilities_surface_is_up_to_date(self):
        result = run("stapel_tools.surface", ".", "--patch", "--check")
        assert result.returncode == 0, result.stdout + result.stderr

    def test_llms_txt_is_up_to_date(self, tmp_path):
        result = run("stapel_tools.llms_txt", ".", "--out", str(tmp_path))
        assert result.returncode == 0, result.stdout + result.stderr
        assert (tmp_path / "llms.txt").read_text() == (
            REPO / "docs" / "llms.txt"
        ).read_text(), "docs/llms.txt is stale — run `make contract` and commit it"

    def test_readme_is_up_to_date(self):
        result = run("stapel_tools.readme", ".", "--check")
        assert result.returncode == 0, result.stdout + result.stderr
