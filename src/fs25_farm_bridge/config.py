import os
from dataclasses import dataclass
from typing import List, Optional

DEFAULT_FEED_BASE_URL_1 = "http://144.126.158.162:9120/feed"
DEFAULT_FEED_CODE_1 = "mt3bqE0kBPlcS8Ld"
DEFAULT_FEED_BASE_URL_2 = "http://144.126.153.108:9110/feed"
DEFAULT_FEED_CODE_2 = "Axa2ixzvN7Gj8bQ3"


@dataclass(frozen=True)
class ServerConfig:
    server_id: int
    name: str
    feed_base_url: str
    feed_code: str
    base44_api_key: str
    cache_file: str

    @property
    def stats_feed_url(self) -> str:
        return f"{self.feed_base_url}/dedicated-server-stats.xml?code={self.feed_code}"

    def savegame_feed_url(self, file_name: str) -> str:
        return (
            f"{self.feed_base_url}/dedicated-server-savegame.html"
            f"?code={self.feed_code}&file={file_name}"
        )


class Config:
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
            f"Server '{selected_server}' not configured. "
            f"Configured: {[s.server_id for s in self.servers]}"
        )

    def _load_servers(self) -> List[ServerConfig]:
        # BASE44_API_KEY is the API key for the agent app (69bd745a05c2dd67076e46de)
        api_key = self._require("BASE44_API_KEY")
        return [
            self._load_server(server_id=1, api_key=api_key),
            self._load_server(server_id=2, api_key=api_key),
        ]

    def _load_server(self, server_id: int, api_key: str) -> ServerConfig:
        suffix = str(server_id)
        feed_base_default = DEFAULT_FEED_BASE_URL_1 if server_id == 1 else DEFAULT_FEED_BASE_URL_2
        feed_code_default = DEFAULT_FEED_CODE_1 if server_id == 1 else DEFAULT_FEED_CODE_2
        feed_base_url = os.environ.get(f"FS25_FEED_BASE_URL_{suffix}", feed_base_default)
        feed_code = os.environ.get(f"FS25_FEED_CODE_{suffix}", feed_code_default)
        default_name = (
            "KAW's farming playground 1" if server_id == 1 else "KAW's farming playground 2"
        )
        server_name = os.environ.get(f"SERVER_NAME_{suffix}", default_name)
        cache_file = os.environ.get(f"CACHE_FILE_{suffix}", f".cache/server{server_id}_state.json")
        return ServerConfig(
            server_id=server_id,
            name=server_name,
            feed_base_url=feed_base_url.rstrip("/"),
            feed_code=feed_code,
            base44_api_key=api_key,
            cache_file=cache_file,
        )

    @staticmethod
    def _require(key: str) -> str:
        value = os.environ.get(key)
        if not value:
            raise EnvironmentError(f"Missing required env var: {key}")
        return value
