import logging
import os
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
    },
    2: {
        "stats": "http://144.126.153.108:9110/feed/dedicated-server-stats.xml?code=Axa2ixzvN7Gj8bQ3",
        "career": "http://144.126.153.108:9110/feed/dedicated-server-savegame.html?code=Axa2ixzvN7Gj8bQ3&file=careerSavegame",
        "economy": "http://144.126.153.108:9110/feed/dedicated-server-savegame.html?code=Axa2ixzvN7Gj8bQ3&file=economy",
    },
}

BASE44_INGEST_URL = os.environ.get(
    "BASE44_API_URL",
    "https://kaws-agent-076e46de.base44.app/functions/farmIntelligenceIngest",
)

def _daytime_to_hhmm(day_time_ms: int) -> str:
    total_seconds = day_time_ms // 1000
    hours = (total_seconds // 3600) % 24
    minutes = (total_seconds % 3600) // 60
    return f"{hours:02d}:{minutes:02d}"


def _parse_stats_xml(root: Any, server_name: str) -> Dict[str, Any]:
    """Parse dedicated-server-stats.xml — PascalCase tags."""
    map_name = root.get("mapName", "")
    day_time_ms = int(root.get("dayTime", "0"))
    current_time = _daytime_to_hhmm(day_time_ms)

    # Players
    slots_el = root.find("Slots")
    players_online = 0
    player_slots = 10
    players_list = []
    if slots_el is not None:
        player_slots = int(slots_el.get("capacity", "10"))
        players_online = int(slots_el.get("numUsed", "0"))
        for p in slots_el.findall("Player"):
            if p.get("isUsed") == "true":
                players_list.append({
                    "name": p.get("name", "Unknown"),
                    "uptime": p.get("uptime", "0"),
                    "isAdmin": p.get("isAdmin", "false") == "true",
                })

    # Farmlands (PascalCase)
    farmlands = []
    farm_ids_from_farmlands: Dict[str, Dict] = {}
    for fl in root.findall(".//Farmland"):
        owner_id = fl.get("owner", "0")
        farmlands.append({
            "id": fl.get("id", ""),
            "name": fl.get("name", ""),
            "owner": owner_id,
            "area": fl.get("area", "0"),
            "price": fl.get("price", "0"),
            "x": fl.get("x", "0"),
            "z": fl.get("z", "0"),
        })
        # Accumulate area per farm for derived farm list
        if owner_id and owner_id != "0":
            if owner_id not in farm_ids_from_farmlands:
                farm_ids_from_farmlands[owner_id] = {"area": 0.0, "count": 0}
            farm_ids_from_farmlands[owner_id]["area"] += float(fl.get("area", "0") or "0")
            farm_ids_from_farmlands[owner_id]["count"] += 1

    # Fields (PascalCase)
    fields = []
    for f in root.findall(".//Field"):
        is_owned = f.get("isOwned", "false") == "true"
        fields.append({
            "fieldId": f.get("id", ""),
            "fruitType": f.get("fruitType", ""),
            "growthState": f.get("growthState", "0"),
            "isOwned": is_owned,
            "weedState": f.get("weedFactor", "0"),
            "sprayLevel": f.get("sprayLevel", "0"),
            "limeLevel": f.get("limeLevel", "0"),
            "plowLevel": f.get("plowLevel", "0"),
            "harvestReady": False,
            "needsAttention": False,
            "assignedFarm": f.get("ownedByFarmId", ""),
            "groundType": "",
            "cropType": "",
            "x": f.get("x", "0"),
            "z": f.get("z", "0"),
        })

    # Vehicles (PascalCase)
    vehicles = []
    for v in root.findall(".//Vehicle"):
        vehicles.append({
            "vehicleName": v.get("name", ""),
            "vehicleType": v.get("type", ""),
            "category": v.get("category", ""),
            "status": "active",
            "operator": v.get("controllerName", ""),
            "fillTypes": v.get("fillTypes", ""),
            "fillLevels": v.get("fillLevels", ""),
            "location": f"{v.get('x','0')},{v.get('z','0')}",
            "assignedFarm": "",
        })

    return {
        "serverName": server_name,
        "map": map_name,
        "playersOnline": str(players_online),
        "slots": str(player_slots),
        "players": players_list,
        "farmlands": farmlands,
        "fields": fields,
        "vehicles": vehicles,
        "currentTime": current_time,
        "_farmIdsFromFarmlands": farm_ids_from_farmlands,
    }


def _parse_career_xml(root: Any, farm_ids_hint: Dict = None) -> List[Dict]:
    """Parse careerSavegame for farm data. Falls back to deriving from farmland ownership."""
    # Try direct farm elements first (lowercase, as in some versions)
    farms = []
    for farm_el in root.findall(".//farm"):
        farms.append({
            "farmId": farm_el.get("farmId", ""),
            "farmName": farm_el.get("name", f"Farm {farm_el.get('farmId','')}"),
            "balance": float(farm_el.get("money", "0") or "0"),
            "loan": float(farm_el.get("loan", "0") or "0"),
            "color": farm_el.get("color", "0"),
            "players": [],
        })

    if farms:
        return farms

    # No direct farm data — derive from farmland ownership
    if farm_ids_hint:
        for fid, info in sorted(farm_ids_hint.items()):
            farms.append({
                "farmId": fid,
                "farmName": f"Farm {fid}",
                "balance": 0,
                "loan": 0,
                "color": "0",
                "players": [],
                "workedHectares": round(info["area"], 2),
            })

    return farms


def _parse_economy_xml(root: Any) -> Dict[str, Any]:
    """Parse economy XML for crop prices."""
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
        crop_prices[crop] = {"priceHistory": history}
    return {"cropPrices": crop_prices}


def _fetch_server_data(server: ServerConfig, timeout: int, retry_attempts: int) -> Dict[str, Any]:
    urls = _SERVER_FEEDS.get(server.server_id)
    if urls is None:
        logger.error("No feed mapping for server_id=%s", server.server_id)
        return {}

    stats_root = fetch_http_xml(urls["stats"], timeout=timeout, retry_attempts=retry_attempts)
    career_root = fetch_http_xml(urls["career"], timeout=timeout, retry_attempts=retry_attempts)
    economy_root = fetch_http_xml(urls["economy"], timeout=timeout, retry_attempts=retry_attempts)

    if stats_root is None:
        logger.error("Stats feed unavailable for server %s", server.server_id)
        return {}

    payload = _parse_stats_xml(stats_root, server.name)
    farm_ids_hint = payload.pop("_farmIdsFromFarmlands", {})
    current_time = payload.pop("currentTime", "")

    farms = _parse_career_xml(career_root, farm_ids_hint) if career_root is not None else list(
        {"farmId": fid, "farmName": f"Farm {fid}", "balance": 0, "loan": 0, "color": "0", "players": []}
        for fid in farm_ids_hint
    )
    payload["farms"] = farms

    economy_data = _parse_economy_xml(economy_root) if economy_root is not None else {}
    payload["economy"] = economy_data

    payload["environment"] = {
        "currentDay": 0,
        "currentTime": current_time,
        "season": "",
        "currentWeather": "",
        "timeSinceLastRain": 0,
        "forecast": [],
    }

    return payload


def _send_to_base44(data: Dict[str, Any]) -> bool:
    url = BASE44_INGEST_URL
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
            return False
    except Exception as exc:
        logger.error("Request to Base44 failed: %s", exc)
        print(f"[ERROR] Request failed: {exc}")
        raise


def run(selected_server: Optional[int] = None, run_all: bool = False) -> None:
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
        print(f"  Farms: {len(data.get('farms', []))}")
        print(f"  Fields: {len(data.get('fields', []))}")
        print(f"  Vehicles: {len(data.get('vehicles', []))}")
        print(f"  Farmlands: {len(data.get('farmlands', []))}")
        print(f"  Players online: {data.get('playersOnline', 0)}")

        _send_to_base44(data)
        logger.info("Done with server %s.", server.server_id)


def fetch_server_data(server_id: int) -> Dict[str, Any]:
    config = Config()
    server = config.get_servers(selected_server=server_id)[0]
    return _fetch_server_data(server, config.request_timeout, config.retry_attempts)
