import logging
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

_FARMER_PROFILE_ENDPOINT = (
    "https://api.base44.com/api/apps/69ac6dca5af2dc4d433b68bd/"
    "entities/FarmerProfile"
)


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
        self.api_url = api_url.rstrip("/") if api_url else _FARMER_PROFILE_ENDPOINT
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.farmer_profile_url = self.api_url or _FARMER_PROFILE_ENDPOINT

        self._session = requests.Session()
        self._session.headers.update(
            {
                "api_key": api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        payload: Optional[Any] = None,
    ) -> Optional[requests.Response]:
        """
        Execute an HTTP request with retry/backoff for transient failures.
        Returns a Response on success, otherwise None.
        """
        for attempt in range(1, self.retry_attempts + 1):
            response: Optional[requests.Response] = None
            try:
                response = self._session.request(
                    method=method,
                    url=url,
                    json=payload,
                    timeout=self.timeout,
                )
                logger.info("%s %s -> %s", method.upper(), url, response.status_code)

                if response.ok:
                    return response

                if 400 <= response.status_code < 500:
                    logger.error(
                        "%s %s failed with %s: %s",
                        method.upper(),
                        url,
                        response.status_code,
                        response.text,
                    )
                    return response

                logger.warning(
                    "Attempt %d/%d failed for %s %s with status %s",
                    attempt,
                    self.retry_attempts,
                    method.upper(),
                    url,
                    response.status_code,
                )

            except requests.exceptions.Timeout:
                logger.warning(
                    "Attempt %d/%d timed out for %s %s",
                    attempt,
                    self.retry_attempts,
                    method.upper(),
                    url,
                )
            except requests.exceptions.RequestException as exc:
                logger.warning(
                    "Request error on attempt %d/%d for %s %s: %s",
                    attempt,
                    self.retry_attempts,
                    method.upper(),
                    url,
                    exc,
                )

            if attempt < self.retry_attempts:
                time.sleep(2**attempt)

        logger.error("All %d attempts failed for %s %s", self.retry_attempts, method, url)
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_farmer_profiles(self) -> List[Dict[str, Any]]:
        response = self._request("GET", self.farmer_profile_url)
        if response is None or not response.ok:
            return []
        try:
            payload = response.json()
        except ValueError:
            logger.error("Invalid JSON response while fetching FarmerProfile entities.")
            return []

        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                return data
        logger.warning("Unexpected FarmerProfile payload shape: %s", type(payload).__name__)
        return []

    def create_farmer_profile(self, profile: Dict[str, Any]) -> bool:
        response = self._request("POST", self.farmer_profile_url, payload=profile)
        if response is None:
            logger.error("Create FarmerProfile failed: no response")
            return False
        if response.ok:
            logger.info("Create FarmerProfile success: status=%s", response.status_code)
            return True
        logger.error("Create FarmerProfile failed: status=%s", response.status_code)
        return False

    def update_farmer_profile(self, entity_id: str, profile: Dict[str, Any]) -> bool:
        url = f"{self.farmer_profile_url}/{entity_id}"
        response = self._request("PUT", url, payload=profile)
        if response is None:
            logger.error("Update FarmerProfile failed (id=%s): no response", entity_id)
            return False
        if response.ok:
            logger.info(
                "Update FarmerProfile success (id=%s): status=%s",
                entity_id,
                response.status_code,
            )
            return True
        logger.error(
            "Update FarmerProfile failed (id=%s): status=%s",
            entity_id,
            response.status_code,
        )
        return False

    def close(self) -> None:
        self._session.close()
