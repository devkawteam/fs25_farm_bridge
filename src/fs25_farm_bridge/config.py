import os


class Config:
    """
    Reads all required runtime settings from environment variables.
    Raises EnvironmentError on startup if any required variable is missing.
    """

    def __init__(self) -> None:
        self.base44_api_url: str = self._require("BASE44_API_URL")
        self.base44_api_key: str = self._require("BASE44_API_KEY")

        self.ftp_host: str = self._require("GPORTAL_FTP_HOST")
        self.ftp_port: int = int(os.environ.get("GPORTAL_FTP_PORT", "51061"))
        self.ftp_user: str = self._require("GPORTAL_FTP_USER")
        self.ftp_pass: str = self._require("GPORTAL_FTP_PASS")

        self.cache_file: str = os.environ.get(
            "CACHE_FILE", ".cache/bridge_state.json"
        )
        self.request_timeout: int = int(os.environ.get("REQUEST_TIMEOUT", "15"))
        self.retry_attempts: int = int(os.environ.get("RETRY_ATTEMPTS", "3"))

    @staticmethod
    def _require(key: str) -> str:
        value = os.environ.get(key)
        if not value:
            raise EnvironmentError(
                f"Required environment variable '{key}' is not set."
            )
        return value
