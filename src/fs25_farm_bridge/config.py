import os
from dataclasses import dataclass
from typing import List, Optional


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
    Reads all required runtime settings from environment variables.
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
        # Multi-server mode: *_1 / *_2 style variables.
        if os.environ.get("GPORTAL_FTP_HOST_1"):
            servers: List[ServerConfig] = []
            for server_id in range(1, 10):
                suffix = str(server_id)
                host = os.environ.get(f"GPORTAL_FTP_HOST_{suffix}")
                if not host:
                    continue

                ftp_port = int(os.environ.get(f"GPORTAL_FTP_PORT_{suffix}", "51061"))
                ftp_user = self._require(f"GPORTAL_FTP_USER_{suffix}")
                ftp_pass = self._require(f"GPORTAL_FTP_PASS_{suffix}")

                # Allow shared Base44 credentials when per-server keys are not set.
                base44_api_url = os.environ.get(
                    f"BASE44_API_URL_{suffix}", os.environ.get("BASE44_API_URL")
                )
                base44_api_key = os.environ.get(
                    f"BASE44_API_KEY_{suffix}", os.environ.get("BASE44_API_KEY")
                )
                if not base44_api_url:
                    raise EnvironmentError(
                        f"Required environment variable 'BASE44_API_URL_{suffix}' "
                        "(or shared 'BASE44_API_URL') is not set."
                    )
                if not base44_api_key:
                    raise EnvironmentError(
                        f"Required environment variable 'BASE44_API_KEY_{suffix}' "
                        "(or shared 'BASE44_API_KEY') is not set."
                    )

                servers.append(
                    ServerConfig(
                        server_id=server_id,
                        name=os.environ.get(
                            f"SERVER_NAME_{suffix}", f"Farm Server {server_id}"
                        ),
                        ftp_host=host,
                        ftp_port=ftp_port,
                        ftp_user=ftp_user,
                        ftp_pass=ftp_pass,
                        base44_api_url=base44_api_url,
                        base44_api_key=base44_api_key,
                        cache_file=os.environ.get(
                            f"CACHE_FILE_{suffix}",
                            f".cache/bridge_state_server{server_id}.json",
                        ),
                    )
                )

            if not servers:
                raise EnvironmentError(
                    "Multi-server mode detected, but no valid server configs found."
                )
            return servers

        # Backward-compatible single-server mode.
        return [
            ServerConfig(
                server_id=1,
                name=os.environ.get("SERVER_NAME", "Farm Server 1"),
                ftp_host=self._require("GPORTAL_FTP_HOST"),
                ftp_port=int(os.environ.get("GPORTAL_FTP_PORT", "51061")),
                ftp_user=self._require("GPORTAL_FTP_USER"),
                ftp_pass=self._require("GPORTAL_FTP_PASS"),
                base44_api_url=self._require("BASE44_API_URL"),
                base44_api_key=self._require("BASE44_API_KEY"),
                cache_file=os.environ.get("CACHE_FILE", ".cache/bridge_state.json"),
            )
        ]

    @staticmethod
    def _require(key: str) -> str:
        value = os.environ.get(key)
        if not value:
            raise EnvironmentError(
                f"Required environment variable '{key}' is not set."
            )
        return value
