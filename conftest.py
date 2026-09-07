"""Root pytest config: makes each component's modules importable without turning them into
installed packages — matches how they actually run (each directory is its own self-contained
unit: producer/, spark_streaming/, dashboard/), so tests import them the same way.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent
for sub in ("producer", "spark_streaming", "dashboard"):
    sys.path.insert(0, str(ROOT / sub))
