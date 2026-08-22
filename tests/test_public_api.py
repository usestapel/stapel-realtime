"""Package-level public API (PEP 562 lazy exports) and import hygiene."""
import os
import subprocess
import sys

import pytest

import stapel_realtime

EXPECTED_API = [
    "realtime_settings",
    "deliver",
    "deliver_frame",
    "revoke",
    "BaseStreamConsumer",
    "EphemeralStreamConsumer",
    "ResumableStreamConsumer",
    "JournalRow",
    "WorkspaceCapability",
    "build_websocket_application",
    "collect_websocket_urlpatterns",
    "StreamKey",
    "InvalidStreamKey",
    "build_stream_key",
    "parse_stream_key",
    "workspace_stream",
    "group_name",
    "WIRE_VERSION",
    "Frame",
    "InvalidEnvelope",
    "frame",
    "parse_frame",
]


class TestLazyExports:
    def test_all_declares_public_api(self):
        assert stapel_realtime.__all__ == EXPECTED_API

    @pytest.mark.parametrize("name", EXPECTED_API)
    def test_every_declared_name_resolves(self, name):
        assert getattr(stapel_realtime, name) is not None

    def test_settings_resolve(self):
        from stapel_realtime.conf import realtime_settings

        assert stapel_realtime.realtime_settings is realtime_settings

    def test_unknown_attribute_raises(self):
        with pytest.raises(AttributeError, match="nonexistent_export"):
            stapel_realtime.nonexistent_export


class TestImportWithoutDjangoSettings:
    def test_package_import_is_django_free(self):
        """`import stapel_realtime` must not import Django nor require settings."""
        env = {k: v for k, v in os.environ.items() if k != "DJANGO_SETTINGS_MODULE"}
        code = (
            "import sys\n"
            "import stapel_realtime\n"
            'polluted = [m for m in sys.modules if m == "django" or m.startswith("django.")]\n'
            'assert not polluted, f"django imported at package import time: {polluted}"\n'
            'polluted = [m for m in sys.modules if m == "channels" or m.startswith("channels.")]\n'
            'assert not polluted, f"channels imported at package import time: {polluted}"\n'
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=env,
            cwd=os.path.dirname(sys.executable),
        )
        assert result.returncode == 0, result.stderr

    def test_the_envelope_needs_neither_django_nor_channels(self):
        """The module a client-side tool or a schema check would import.

        `streams` is deliberately NOT in this list: it re-exports the core's
        stream-key builder rather than growing a second regex, and the core's
        comm package is a Django citizen. One canon beats one fewer import.
        """
        env = {k: v for k, v in os.environ.items() if k != "DJANGO_SETTINGS_MODULE"}
        code = (
            "import sys\n"
            "from stapel_realtime import envelope\n"
            "assert envelope.frame('ping')['v'] == 1\n"
            'bad = [m for m in sys.modules if m.split(".")[0] in ("django", "channels")]\n'
            'assert not bad, bad\n'
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=env,
            cwd=os.path.dirname(sys.executable),
        )
        assert result.returncode == 0, result.stderr


class TestOptionalChannelsDependency:
    def test_consumers_say_which_extra_is_missing(self):
        """Without the extra the error must name the extra, not 'No module named channels'."""
        code = (
            "import sys\n"
            "import builtins\n"
            "real_import = builtins.__import__\n"
            "def fake(name, *a, **k):\n"
            "    if name == 'channels' or name.startswith('channels.'):\n"
            "        raise ImportError('No module named channels')\n"
            "    return real_import(name, *a, **k)\n"
            "builtins.__import__ = fake\n"
            "for mod in [m for m in sys.modules if m.startswith('channels')]:\n"
            "    del sys.modules[mod]\n"
            "try:\n"
            "    import stapel_realtime.consumers\n"
            "except ImportError as exc:\n"
            "    assert 'stapel-realtime[channels]' in str(exc), exc\n"
            "else:\n"
            "    raise AssertionError('expected ImportError')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr
