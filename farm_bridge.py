"""
farm_bridge.py — root-level entry point for direct execution.

Usage:
    python farm_bridge.py

Prefer running via the module interface instead:
    PYTHONPATH=src python -m fs25_farm_bridge
"""
import os
import sys

# Make the src layout work when this script is run directly from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from fs25_farm_bridge.__main__ import main  # noqa: E402

if __name__ == "__main__":
    main()
