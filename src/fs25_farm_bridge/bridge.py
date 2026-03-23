import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests

from .config import Config, ServerConfig
from fs25_farm_bridge.utils import (
    fetch_http_xml,
    parse_economy,
    parse_environment,
    parse_farms,
    parse_fields,
    parse_players,
    parse_vehicles,
)

logger = logging.getLogger(__name__)

_SERVER_FEEDS = {
    1: {
        "stats": "http://144.126.158.162:9120/feed/dedicated-server-stats.xml?code=mt3bqE0kBPlcS8Ld",
        "career": "http://144.126.158.162:9120/feed/dedicated-server-savegame.html?code=mt3bqE0kBPlcS8Ld&file=careerSavegame",
        "economy": "http://144.126.158.162:9120/feed/dedicated-server-savegame.html?code=mt3bqE0kBPlcS8Ld&file=economy",
        "vehicles": "http://144.126.158.162:9120/feed/dedicated-server-savegame.html?code=mt3bqE0kBPlcS8Ld&file=vehicles",
    },
    2: {
        "stats": "http://144.126.153.108:9110/feed/dedicated-server-stats.xml?code=Axa2ixzvN7Gj8bQ3",
        "career": "http://144.126.153.108:9110/feed/dedicated-server-savegame.html?code=Axa2ixzvN7Gj8bQ3&file=careerSavegame",
        "economy": "http://144.126.153.108:9110/feed/dedicated-server-savegame.html?code=Axa2ixzvN7Gj8bQ3&file=economy",
        "vehicles": "http://144.126.153.108:9110/feed/dedicated-server-savegame.html?code=Axa2ixzvN7Gj8bQ3&file=vehicles",
    },
}

BASE44_INGEST_URL = os.environ.get(
    "BASE44_API_URL",
    "https://kaws-agent-076e46de.base44.app/functions/farmIntelligenceIngest",
)


def _fetch_server_data(server: ServerConfig, timeout: int, retry_attempts: int) -> Dict[str, Any]:
    urls = _SERVER_FEEDS.get(server.server_id)
    if urls is None:
        logger.error("No HTTP feed mapping for server_id=%s", server.server_id)
        return {}

    stats_root = fetch_http_xml(urls["stats"], timeout=timeout, retry_attempts=retry_attempts)
    career_root = fetch_http_xml(urls["career"], timeout=timeout, retry_attempts=retry_attempts)
    economy_root = fetch_http_xml(urls["economy"], timeout=timeout, retry_attempts=retry_attempts)
    vehicles_root = fetch_http_xml(urls["vehicles"], timeout=timeout, retry_attempts=retry_attempts)

    environment = parse_environment(stats_root) if stats_root is not None else {}
    players_live = parse_players(stats_root) if stats_root is not None else []
    farms = parse_farms(career_root) if career_root is not None else []
    fields = parse_fields(career_root) if career_root is not None else []
    vehicles = parse_vehicles(vehicles_root) if vehicles_root is not None else []
    economy = parse_economy(economy_root) if economy_root is not None else {}

    # Pull players online from stats feed Slots element
    players_online = 0
    player_slots = 10
    if stats_root is not None:
        slots_el = stats_root.find("Slots")
        if slots_el is not None:
            player_slots = int(slots_el.get("capacity", "10"))
            players_online = int(slots_el.get("numUsed", "0"))

    return {
        "serverName": server.name,
        "map": "",
        "playersOnline": str(players_online),
        "slots": str(player_slots),
        "players": players_live,
        "farms": farms,
        "fields": fields,
        "vehicles": vehicles,
        "farmlands": [],
        "environment": environment,
        "economy": economy,
        "serverId": server.server_id,
    }


def _send_to_base44(data: Dict[str, Any]) -> bool:
    url = BASE44_INGEST_URL
    logger.info("Sending data for server '%s' to Base44 ingest...", data.get("serverName"))
    try:
        response = requests.post(
            url,
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=300,
        )
        logger.info("Response: %s", response.status_code)
        if response.ok:
            logger.info("Ingest result: %s", response.json())
            print(f"[OK] Server '{data.get('serverName')}' synced: {response.json()}")
            return True
        else:
            logger.error("Ingest failed: %s %s", response.status_code, response.text[:300])
            print(f"[ERROR] Server '{data.get('serverName')}': {response.status_code} {response.text[:200]}")
            return False
    except Exception as exc:
        logger.error("Request to Base44 failed: %s", exc)
        print(f"[ERROR] Request failed: {exc}")
        return False


def run(selected_server: Optional[int] = None, run_all: bool = False) -> None:
    """Top-level bridge run: fetch → parse → push to Base44."""
    config = Config()
    servers = config.get_servers(selected_server=selected_server, run_all=run_all)

    for server in servers:
        logger.info("Starting sync for server %s (%s)", server.server_id, server.name)
        print(f"\n--- Syncing Server {server.server_id}: {server.name} ---")

        data = _fetch_server_data(
            server=server,
            timeout=config.request_timeout,
            retry_attempts=config.retry_attempts,
        )

        if not data:
            print(f"[SKIP] No data fetched for server {server.server_id}")
            continue

        print(f"  Farms: {len(data.get('farms', []))}")
        print(f"  Fields: {len(data.get('fields', []))}")
        print(f"  Vehicles: {len(data.get('vehicles', []))}")
        print(f"  Players online: {data.get('playersOnline', 0)}")

        _send_to_base44(data)
        logger.info("Done with server %s.", server.server_id)


# Keep this so any remaining imports of fetch_server_data still work
def fetch_server_data(server_id: int) -> Dict[str, Any]:
    config = Config()
    server = config.get_servers(selected_server=server_id)[0]
    return _fetch_server_data(server, config.request_timeout, config.retry_attempts)
