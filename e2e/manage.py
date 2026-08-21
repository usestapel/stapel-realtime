#!/usr/bin/env python
"""manage.py for the realtime e2e host (see e2e/settings.py)."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "e2e.settings")
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
