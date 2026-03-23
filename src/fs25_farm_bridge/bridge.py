import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests

from .config import Config, ServerConfig
from fs25_farm_bridge.utils import fetch_http_xml

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

MS_PER_DAY = 86400000


def _daytime_to_hhmm(day_time_ms: int) -> str:
    """Convert FS25 dayTime (ms since midnight) to HH:MM string."""
    total_seconds = day_time_ms // 1000
    hours = (total_seconds // 3600) % 24
    minutes = (total_seconds % 3600) // 60
    return f"{hours:02d}:{minutes:02d}"


def _parse_stats_xml(root: Any, server_name: str) -> Dict[str, Any]:
    """Parse the dedicated-server-stats.xml into our ingest payload format."""
    import xml.etree.ElementTree as ET

    # Root attrs
    map_name = root.get("mapName", "")
    day_time_ms = int(root.get("dayTime", "0"))
    current_time = _daytime_to_hhmm(day_time_ms)

    # Players
    slots_el = root.find("Slots")
    players_online = 0
    player_slots = 10
    players = []
    if slots_el is not None:
        player_slots = int(slots_el.get("capacity", "10"))
        players_online = int(slots_el.get("numUsed", "0"))
        for p in slots_el.findall("Player"):
            if p.get("isUsed") == "true":
                players.append({
                    "name": p.get("name", "Unknown"),
                    "uptime": p.get("uptime", "0"),
                    "isAdmin": p.get("isAdmin", "false") == "true",
                })

    # Farmlands
    farmlands = []
    for fl in root.findall(".//Farmland"):
        farmlands.append({
            "id": fl.get("id", ""),
            "name": fl.get("name", ""),
            "owner": fl.get("owner", "0"),
            "area": fl.get("area", "0"),
            "price": fl.get("price", "0"),
            "x": fl.get("x", "0"),
            "z": fl.get("z", "0"),
        })

    # Fields
    fields = []
    for f in root.findall(".//Field"):
        fields.append({
            "fieldId": f.get("id", ""),
            "fruitType": f.get("fruitType", "UNKNOWN"),
            "growthState": f.get("growthState", "0"),
            "isOwned": f.get("isOwned", "false") == "true",
            "weedState": f.get("weedFactor", "0"),
            "sprayLevel": f.get("sprayLevel", "0"),
            "limeLevel": f.get("limeLevel", "0"),
            "plowLevel": f.get("plowLevel", "0"),
            "harvestReady": False,
            "needsAttention": False,
            "assignedFarm": f.get("ownedByFarmId", ""),
            "groundType": "",
        })

    # Vehicles
    vehicles = []
    for v in root.findall(".//Vehicle"):
        vehicles.append({
            "name": v.get("name", ""),
            "type": v.get("type", ""),
            "category": v.get("category", ""),
            "x": v.get("x", "0"),
            "z": v.get("z", "0"),
            "controller": v.get("controllerName", ""),
            "isAbandoned": False,
            "fillTypes": "",
            "fillLevels": "",
        })

    return {
        "serverName": server_name,
        "map": map_name,
        "playersOnline": str(players_online),
        "slots": str(player_slots),
        "players": players,
        "farmlands": farmlands,
        "fields": fields,
        "vehicles": vehicles,
        "currentTime": current_time,
    }


def _parse_career_xml(root: Any) -> Dict[str, Any]:
    """Parse careerSavegame for farm financials and settings."""
    import xml.etree.ElementTree as ET

    farms = []
    for farm_el in root.findall(".//farm"):
        money = farm_el.get("money", "0")
        farms.append({
            "farmId": farm_el.get("farmId", ""),
            "farmName": farm_el.get("name", f"Farm {farm_el.get('farmId','')}"),
            "balance": float(money) if money else 0,
            "loan": float(farm_el.get("loan", "0")),
            "color": farm_el.get("color", "0"),
            "players": [],
            "stats": {},
        })

    return {"farms": farms}


def _parse_economy_xml(root: Any, season_hint: str = "SPRING") -> Dict[str, Any]:
    """Parse economy XML for crop prices."""
    period_map = {
        "SPRING": "EARLY_SPRING",
        "SUMMER": "EARLY_SUMMER",
        "AUTUMN": "EARLY_AUTUMN",
        "WINTER": "EARLY_WINTER",
    }
    period = period_map.get(season_hint, "EARLY_SPRING")

    crop_prices: Dict[str, Any] = {}
    for fill_el in root.findall(".//fillType"):
        crop = fill_el.get("fillType", "")
        if not crop or crop == "UNKNOWN":
            continue
        history: Dict[str, float] = {}
        for p in fill_el.findall(".//period"):
            pname = p.get("period", "")
            val = float(p.get("value", "0") or "0")
            if pname:
                history[pname] = val
        crop_prices[crop] = {"totalSold": 0, "priceHistory": history}

    return {"cropPrices": crop_prices}


def _fetch_server_data(server: ServerConfig, timeout: int, retry_attempts: int) -> Dict[str, Any]:
    urls = _SERVER_FEEDS.get(server.server_id)
    if urls is None:
        logger.error("No feed mapping for server_id=%s", server.server_id)
        return {}

    # Fetch all feeds
    stats_root = fetch_http_xml(urls["stats"], timeout=timeout, retry_attempts=retry_attempts)
    career_root = fetch_http_xml(urls["career"], timeout=timeout, retry_attempts=retry_attempts)
    economy_root = fetch_http_xml(urls["economy"], timeout=timeout, retry_attempts=retry_attempts)

    if stats_root is None:
        logger.error("Stats feed unavailable for server %s", server.server_id)
        return {}

    # Build payload from stats XML (main data source)
    payload = _parse_stats_xml(stats_root, server.name)

    # Add career data (farms/finances)
    career_data = _parse_career_xml(career_root) if career_root is not None else {"farms": []}
    payload["farms"] = career_data["farms"]

    # Add economy data (crop prices)
    economy_data = _parse_economy_xml(economy_root) if economy_root is not None else {}
    payload["economy"] = economy_data

    # Build environment block from available data
    payload["environment"] = {
        "currentDay": 0,
        "currentTime": payload.pop("currentTime", ""),
        "season": "",
        "currentWeather": "",
        "timeSinceLastRain": 0,
        "forecast": [],
    }

    return payload


def _send_to_base44(data: Dict[str, Any]) -> bool:
    url = BASE44_INGEST_URL
    logger.info("Sending data for '%s' to Base44 ingest...", data.get("serverName"))
    try:
        response = requests.post(
            url,
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=300,
        )
        if response.ok:
            result = response.json()
            print(f"[OK] '{data.get('serverName')}' synced: {result}")
            return True
        else:
            print(f"[ERROR] '{data.get('serverName')}': {response.status_code} {response.text[:300]}")
            response.raise_for_status()
            return False
    except Exception as exc:
        logger.error("Request to Base44 failed: %s", exc)
        print(f"[ERROR] Request failed: {exc}")
        raise


def run(selected_server: Optional[int] = None, run_all: bool = False) -> None:
    """Top-level bridge run: fetch → parse → push to Base44."""
    config = Config()
    servers = config.get_servers(selected_server=selected_server, run_all=run_all)

    for server in servers:
        print(f"\n--- Syncing Server {server.server_id}: {server.name} ---")
        data = _fetch_server_data(
            server=server,
            timeout=config.request_timeout,
            retry_attempts=config.retry_attempts,
        )
        if not data:
            print(f"[SKIP] No data for server {server.server_id}")
            continue

        print(f"  Map: {data.get('map', '?')}")
        print(f"  Fields: {len(data.get('fields', []))}")
        print(f"  Vehicles: {len(data.get('vehicles', []))}")
        print(f"  Farmlands: {len(data.get('farmlands', []))}")
        print(f"  Farms: {len(data.get('farms', []))}")
        print(f"  Players online: {data.get('playersOnline', 0)}")

        _send_to_base44(data)
        logger.info("Done with server %s.", server.server_id)


def fetch_server_data(server_id: int) -> Dict[str, Any]:
    config = Config()
    server = config.get_servers(selected_server=server_id)[0]
    return _fetch_server_data(server, config.request_timeout, config.retry_attempts)
