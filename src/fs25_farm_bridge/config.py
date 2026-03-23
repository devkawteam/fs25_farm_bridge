import os
from dataclasses import dataclass
from typing import List, Optional


DEFAULT_BASE44_API_URL = (
    "https://api.base44.com/api/apps/69ac6dca5af2dc4d433b68bd/entities/FarmerProfile"
)


@dataclass(frozen=True)
class ServerConfig:
    server_id: int
    name: str
    ftp_host: str
    ftp_port: int
    ftp_user: str
    ftp_pass: str
    base44_api_url: str
    base44_api_key: str
    cache_file: str


class Config:
    """
    Reads runtime settings for a strict multi-server setup (server 1 + 2).
    Raises EnvironmentError on startup if any required variable is missing.
    """

    def __init__(self) -> None:
        self.request_timeout: int = int(os.environ.get("REQUEST_TIMEOUT", "15"))
        self.retry_attempts: int = int(os.environ.get("RETRY_ATTEMPTS", "3"))
        self.servers: List[ServerConfig] = self._load_servers()

    def get_servers(
        self,
        selected_server: Optional[int] = None,
        run_all: bool = False,
    ) -> List[ServerConfig]:
        if run_all:
            return self.servers

        if selected_server is None:
            return [self.servers[0]]

        for server in self.servers:
            if server.server_id == selected_server:
                return [server]

        raise EnvironmentError(
            f"Server '{selected_server}' is not configured. "
            f"Configured servers: {[s.server_id for s in self.servers]}"
        )

    def _load_servers(self) -> List[ServerConfig]:
        base44_api_key = self._require("BASE44_API_KEY")

        return [
            self._load_server(server_id=1, base44_api_key=base44_api_key),
            self._load_server(server_id=2, base44_api_key=base44_api_key),
        ]

    def _load_server(self, server_id: int, base44_api_key: str) -> ServerConfig:
        if server_id not in (1, 2):
            raise EnvironmentError(f"Unsupported server_id '{server_id}'. Use 1 or 2.")

        suffix = str(server_id)
        ftp_host = self._require(f"GPORTAL_FTP_HOST_{suffix}")
        ftp_port = int(os.environ.get(f"GPORTAL_FTP_PORT_{suffix}", "51061"))
        ftp_user = self._require(f"GPORTAL_FTP_USER_{suffix}")
        ftp_pass = self._require(f"GPORTAL_FTP_PASS_{suffix}")

        default_name = (
            "KAW's farming playground 1"
            if server_id == 1
            else "KAW's farming playground 2"
        )
        server_name = os.environ.get(f"SERVER_NAME_{suffix}", default_name)

        cache_default = f".cache/server{server_id}_state.json"
        cache_file = os.environ.get(f"CACHE_FILE_{suffix}", cache_default)

        # Keep URL configurable while defaulting to the FarmerProfile entity endpoint.
        base44_api_url = os.environ.get("BASE44_API_URL", DEFAULT_BASE44_API_URL)

        return ServerConfig(
            server_id=server_id,
            name=server_name,
            ftp_host=ftp_host,
            ftp_port=ftp_port,
            ftp_user=ftp_user,
            ftp_pass=ftp_pass,
            base44_api_url=base44_api_url,
            base44_api_key=base44_api_key,
            cache_file=cache_file,
        )

    @staticmethod
    def _require(key: str) -> str:
        value = os.environ.get(key)
        if not value:
            raise EnvironmentError(f"Missing {key}")
        return value
