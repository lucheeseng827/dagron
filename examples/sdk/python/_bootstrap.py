"""Make the in-tree dagron SDK importable when it isn't pip-installed.

These examples are meant to run straight from a checkout (`python examples/sdk/
python/01_quickstart.py`) without `pip install` first. Importing this module
puts `sdks/python` on `sys.path` *only if* `dagron` isn't already importable, so
an installed `dagron-sdk` always wins.

In your own code you don't need this — just `pip install dagron-sdk` (see
`sdks/python/README.md`) and `import dagron`.
"""
from __future__ import annotations

import importlib.util
import os
import sys


def ensure_dagron_importable() -> None:
    if importlib.util.find_spec("dagron") is not None:
        return
    here = os.path.dirname(os.path.abspath(__file__))
    sdk_dir = os.path.normpath(os.path.join(here, "..", "..", "..", "sdks", "python"))
    if sdk_dir not in sys.path:
        sys.path.insert(0, sdk_dir)


ensure_dagron_importable()
