import logging
import os
import datetime
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

# ServerHub app (kawsplayground.online) - single destination
_APP_ID = "69ac6dca5af2dc4d433b68bd"
_BASE_URL = f"https://api.base44.com/api/apps/{_APP_ID}/entities"

# Map server_id -> playground label used in serverName field
_SERVER_NAMES = {
    1: "KAW's farming playground 1",
    2: "KAW's farming playground 2",
}


def _daytime_to_hhmm(day_time_ms: int) -> str:
    total_seconds = day_time_ms // 1000
    hours = (total_seconds // 3600) % 24
    minutes = (total_seconds % 3600) // 60
    return f"{hours:02d}:{minutes:02d}"


def _api_session(api_key: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "api_key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    return s


def _upsert(session: requests.Session, entity: str, query_field: str, query_val: str, data: dict) -> bool:
    """Find record by query_field=query_val, update if exists, create if not."""
    try:
        resp = session.get(f"{_BASE_URL}/{entity}", params={query_field: query_val}, timeout=15)
        resp.raise_for_status()
        body = resp.json()
        records = body if isinstance(body, list) else body.get("records", [])
        if records:
            rec_id = records[0]["id"]
            r2 = session.put(f"{_BASE_URL}/{entity}/{rec_id}", json=data, timeout=15)
            r2.raise_for_status()
            print(f"  [OK] Updated {entity} ({query_val})")
        else:
            r2 = session.post(f"{_BASE_URL}/{entity}", json=data, timeout=15)
            r2.raise_for_status()
            print(f"  [OK] Created {entity} ({query_val})")
        return True
    except Exception as e:
        print(f"  [ERR] {entity} ({query_val}): {e}")
        logger.error("Upsert %s failed: %s", entity, e)
        return False


def _bulk_replace(session: requests.Session, entity: str, server_name: str, records: list) -> None:
    """Delete all records for this server, then bulk insert new ones."""
    try:
        # Fetch existing IDs for this server
        resp = session.get(f"{_BASE_URL}/{entity}", params={"serverName": server_name}, timeout=15)
        resp.raise_for_status()
        body = resp.json()
        existing = body if isinstance(body, list) else body.get("records", [])
        for rec in existing:
            session.delete(f"{_BASE_URL}/{entity}/{rec['id']}", timeout=10)

        # Insert new records
        for rec in records:
            rec["serverName"] = server_name
            session.post(f"{_BASE_URL}/{entity}", json=rec, timeout=10)

        print(f"  [OK] Replaced {entity}: {len(records)} records")
    except Exception as e:
        print(f"  [ERR] Bulk replace {entity}: {e}")
        logger.error("Bulk replace %s failed: %s", entity, e)


def _parse_stats_xml(root: Any, server_name: str) -> Dict[str, Any]:
    map_name = root.get("mapName", "")
    day_time_ms = int(root.get("dayTime", "0"))
    current_time = _daytime_to_hhmm(day_time_ms)
    current_day = int(root.get("dayLight", "1") or "1")

    slots_el = root.find("Slots")
    players_online = 0
    player_slots = 16
    players_list = []
    if slots_el is not None:
        player_slots = int(slots_el.get("capacity", "16"))
        players_online = int(slots_el.get("numUsed", "0"))
        for p in slots_el.findall("Player"):
            if p.get("isUsed") == "true":
                players_list.append({
                    "name": p.get("name", "Unknown"),
                    "uptime": p.get("uptime", "0"),
                    "isAdmin": p.get("isAdmin", "false") == "true",
                })

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
        if owner_id and owner_id != "0":
            if owner_id not in farm_ids_from_farmlands:
                farm_ids_from_farmlands[owner_id] = {"area": 0.0, "count": 0}
            farm_ids_from_farmlands[owner_id]["area"] += float(fl.get("area", "0") or "0")
            farm_ids_from_farmlands[owner_id]["count"] += 1

    fields = []
    for f in root.findall(".//Field"):
        fields.append({
            "fieldId": f.get("id", ""),
            "fruitType": f.get("fruitType", ""),
            "growthState": f.get("growthState", "0"),
            "isOwned": f.get("isOwned", "false") == "true",
            "weedState": f.get("weedFactor", "0"),
            "sprayLevel": f.get("sprayLevel", "0"),
            "limeLevel": f.get("limeLevel", "0"),
            "plowLevel": f.get("plowLevel", "0"),
            "assignedFarm": f.get("ownedByFarmId", ""),
        })

    vehicles = []
    for v in root.findall(".//Vehicle"):
        vehicles.append({
            "vehicleName": v.get("name", ""),
            "vehicleType": v.get("type", ""),
            "category": v.get("category", ""),
            "operator": v.get("controllerName", ""),
            "fillTypes": v.get("fillTypes", ""),
            "fillLevels": v.get("fillLevels", ""),
            "location": f"{v.get('x','0')},{v.get('z','0')}",
            "status": "active" if v.get("controllerName") else "idle",
        })

    return {
        "serverName": server_name,
        "mapName": map_name,
        "playersOnline": players_online,
        "playerSlots": player_slots,
        "currentDay": current_day,
        "currentTime": current_time,
        "players": players_list,
        "farmlands": farmlands,
        "fields": fields,
        "vehicles": vehicles,
        "_farmIdsFromFarmlands": farm_ids_from_farmlands,
    }


def _parse_career_xml(root: Any, farm_ids_hint: Dict = None) -> List[Dict]:
    farms = []
    for farm_el in root.findall(".//farm"):
        farms.append({
            "farmId": farm_el.get("farmId", ""),
            "farmName": farm_el.get("name", f"Farm {farm_el.get('farmId','')}"),
            "balance": float(farm_el.get("money", "0") or "0"),
            "loan": float(farm_el.get("loan", "0") or "0"),
            "color": farm_el.get("color", "0"),
        })
    if not farms and farm_ids_hint:
        for fid in sorted(farm_ids_hint.keys()):
            farms.append({
                "farmId": fid,
                "farmName": f"Farm {fid}",
                "balance": 0,
                "loan": 0,
                "color": "0",
            })
    return farms


def _parse_economy_xml(root: Any) -> List[Dict]:
    prices = []
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
        if history:
            price_per_liter = next(iter(history.values()), 0)
            price_ton = round(price_per_liter * 1000)
            if price_ton > 0:
                prices.append({
                    "crop_type": crop,
                    "price_per_liter": price_per_liter,
                    "price_per_ton": price_ton,
                    "recorded_date": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                })
    return prices


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

    farms = _parse_career_xml(career_root, farm_ids_hint) if career_root is not None else []
    payload["farms"] = farms
    payload["crop_prices"] = _parse_economy_xml(economy_root) if economy_root is not None else []

    return payload


def _sync_to_serverhub(data: Dict[str, Any], api_key: str) -> None:
    """Write all server data to the ServerHub app entities."""
    server_name = data.get("serverName", "")
    session = _api_session(api_key)

    try:
        # 1. ServerStatus — one record per server
        _upsert(session, "ServerStatus", "serverName", server_name, {
            "serverName": server_name,
            "mapName": data.get("mapName", ""),
            "playersOnline": data.get("playersOnline", 0),
            "playerSlots": data.get("playerSlots", 16),
            "currentDay": data.get("currentDay", 0),
            "currentTime": data.get("currentTime", ""),
            "season": "",
            "currentWeather": "",
            "timeSinceLastRain": 0,
            "forecast": [],
        })

        # 2. Farms — one record per farm per server
        for farm in data.get("farms", []):
            farm_key = f"{server_name}::farm::{farm['farmId']}"
            _upsert(session, "Farm", "farmId", farm_key, {
                "farmId": farm_key,
                "farmName": farm.get("farmName", ""),
                "color": str(farm.get("color", "0")),
                "balance": farm.get("balance", 0),
                "loan": farm.get("loan", 0),
                "players": [],
                "serverName": server_name,
            })

        # 3. Fields — bulk replace (too many to diff individually)
        field_records = []
        for f in data.get("fields", []):
            field_records.append({
                "fieldId": f.get("fieldId", ""),
                "fruitType": f.get("fruitType", ""),
                "cropType": f.get("fruitType", ""),
                "growthState": int(f.get("growthState", 0) or 0),
                "groundType": "",
                "harvestReady": int(f.get("growthState", 0) or 0) >= 6,
                "weedState": float(f.get("weedState", 0) or 0),
                "sprayLevel": float(f.get("sprayLevel", 0) or 0),
                "limeLevel": float(f.get("limeLevel", 0) or 0),
                "plowLevel": float(f.get("plowLevel", 0) or 0),
                "needsAttention": float(f.get("weedState", 0) or 0) > 0.5,
                "isOwned": f.get("isOwned", False),
                "assignedFarm": f.get("assignedFarm", ""),
            })
        _bulk_replace(session, "Field", server_name, field_records)

        # 4. Vehicles — bulk replace
        vehicle_records = []
        for v in data.get("vehicles", [])[:150]:
            loc = (v.get("location") or "0,0").split(",")
            vehicle_records.append({
                "vehicleName": v.get("vehicleName", ""),
                "vehicleType": v.get("vehicleType", ""),
                "category": v.get("category", ""),
                "status": v.get("status", "idle"),
                "operator": v.get("operator", ""),
                "fillTypes": v.get("fillTypes", ""),
                "fillLevels": v.get("fillLevels", ""),
                "location": v.get("location", ""),
                "assignedFarm": "",
            })
        _bulk_replace(session, "Vehicle", server_name, vehicle_records)

        # 5. PlayerActivity — log current online players
        for player in data.get("players", []):
            session.post(f"{_BASE_URL}/PlayerActivity", json={
                "playerName": player.get("name", ""),
                "activityType": "login",
                "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "notes": f"uptime={player.get('uptime','0')} admin={player.get('isAdmin',False)} server={server_name}",
                "serverName": server_name,
            }, timeout=10)

        print(f"\n✓ {server_name} synced to ServerHub.")

    finally:
        session.close()


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

        print(f"  Map: {data.get('mapName', '?')}")
        print(f"  Farms: {len(data.get('farms', []))}")
        print(f"  Fields: {len(data.get('fields', []))}")
        print(f"  Vehicles: {len(data.get('vehicles', []))}")
        print(f"  Players online: {data.get('playersOnline', 0)}")

        _sync_to_serverhub(data, server.base44_api_key)

        logger.info("Done with server %s.", server.server_id)


def fetch_server_data(server_id: int) -> Dict[str, Any]:
    config = Config()
    server = config.get_servers(selected_server=server_id)[0]
    return _fetch_server_data(server, config.request_timeout, config.retry_attempts)
