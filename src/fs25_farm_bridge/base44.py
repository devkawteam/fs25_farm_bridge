import logging
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class Base44Client:
    """
    HTTP client for the Base44 API.

    * Uses a persistent requests.Session with auth headers pre-set.
    * Retries transient errors up to *retry_attempts* times with
      exponential back-off (2 s, 4 s, 8 s …).
    * Never raises on API errors — logs them and returns False instead.
    """

    def __init__(
        self,
        api_url: str,
        api_key: str,
        timeout: int = 15,
        retry_attempts: int = 3,
    ) -> None:
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout
        self.retry_attempts = retry_attempts

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _post(self, endpoint: str, payload: Any) -> bool:
        """
        POST *payload* as JSON to *endpoint*.
        Returns True on success, False if all retry attempts are exhausted.
        """
        url = f"{self.api_url}/{endpoint.lstrip('/')}"

        for attempt in range(1, self.retry_attempts + 1):
            response: Optional[requests.Response] = None
            try:
                response = self._session.post(url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                logger.info("POST %s -> %s", url, response.status_code)
                return True

            except requests.exceptions.Timeout:
                logger.warning(
                    "Attempt %d/%d timed out for %s", attempt, self.retry_attempts, url
                )

            except requests.exceptions.HTTPError as exc:
                status = response.status_code if response is not None else "?"
                logger.error(
                    "HTTP %s on attempt %d/%d for %s: %s",
                    status,
                    attempt,
                    self.retry_attempts,
                    url,
                    exc,
                )
                # 4xx errors are client errors — retrying won't help
                if response is not None and 400 <= response.status_code < 500:
                    break

            except requests.exceptions.RequestException as exc:
                logger.warning(
                    "Request error on attempt %d/%d for %s: %s",
                    attempt,
                    self.retry_attempts,
                    url,
                    exc,
                )

            if attempt < self.retry_attempts:
                time.sleep(2**attempt)

        logger.error("All %d attempts failed for %s", self.retry_attempts, url)
        return False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send_environment(self, data: Dict[str, Any]) -> bool:
        return self._post("environment", data)

    def send_farms(self, farms: List[Dict[str, Any]]) -> bool:
        return self._post("farms", {"farms": farms})

    def send_fields(self, fields: List[Dict[str, Any]]) -> bool:
        return self._post("fields", {"fields": fields})

    def send_economy(self, data: Dict[str, Any]) -> bool:
        return self._post("economy", data)

    def send_players(self, players: List[Dict[str, Any]]) -> bool:
        return self._post("players", {"players": players})

    def close(self) -> None:
        self._session.close()
