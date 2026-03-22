import hashlib
import json
import logging
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _hash_data(data: Any) -> str:
    """Return a stable SHA-256 hash for any JSON-serialisable value."""
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


class BridgeState:
    """
    Persists per-item hashes in a local JSON cache file so that only
    genuinely changed farms, fields, or global sections are forwarded
    to the Base44 API on each run.
    """

    def __init__(self, cache_file: str) -> None:
        self.cache_file = cache_file
        self._state: Dict[str, str] = self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> Dict[str, str]:
        if not os.path.exists(self.cache_file):
            return {}
        try:
            with open(self.cache_file, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read cache state (%s). Starting fresh.", exc)
            return {}

    def save(self) -> None:
        """Write the current hash map back to disk."""
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, "w", encoding="utf-8") as fh:
            json.dump(self._state, fh, indent=2)

    # ------------------------------------------------------------------
    # Change detection
    # ------------------------------------------------------------------

    def has_changed(self, key: str, data: Any) -> bool:
        """
        Return True if *data* differs from the last stored hash for *key*.
        Also updates the stored hash when True is returned so the next call
        with the same, unchanged data returns False.
        """
        new_hash = _hash_data(data)
        if self._state.get(key) == new_hash:
            return False
        self._state[key] = new_hash
        return True
