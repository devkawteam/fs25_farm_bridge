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

# This agent app on Base44 — receives data from both servers
_APP_ID = "69bd745a05c2dd67076e46de"
_BASE_URL = f"https://app.base44.com/api/apps/{_APP_ID}/entities"


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
    try:
        # Fetch and delete existing records for this server
        resp = session.get(f"{_BASE_URL}/{entity}", params={"serverName": server_name}, timeout=15)
        resp.raise_for_status()
        body = resp.json()
        existing = body if isinstance(body, list) else body.get("records", [])
        for rec in existing:
            session.delete(f"{_BASE_URL}/{entity}/{rec['id']}", timeout=10)

        # Insert new records in batches to avoid timeouts
        for rec in records:
            rec["serverName"] = server_name
            r = session.post(f"{_BASE_URL}/{entity}", json=rec, timeout=10)
            if not r.ok:
                logger.warning("Insert %s failed: %s", entity, r.status_code)

        print(f"  [OK] Replaced {entity}: {len(records)} records for {server_name}")
    except Exception as e:
        print(f"  [ERR] Bulk replace {entity}: {e}")
        logger.error("Bulk replace %s failed: %s", entity, e)


def _parse_stats_xml(root: Any, server_name: str) -> Dict[str, Any]:
    map_name = root.get("mapName", "")
    day_time_ms = int(root.get("dayTime", "0"))
    current_time = _daytime_to_hhmm(day_time_ms)

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
            "growthState": int(f.get("growthState", "0") or "0"),
            "isOwned": f.get("isOwned", "false") == "true",
            "weedState": float(f.get("weedFactor", "0") or "0"),
            "sprayLevel": float(f.get("sprayLevel", "0") or "0"),
            "limeLevel": float(f.get("limeLevel", "0") or "0"),
            "plowLevel": float(f.get("plowLevel", "0") or "0"),
            "harvestReady": int(f.get("growthState", "0") or "0") >= 6,
            "needsAttention": float(f.get("weedFactor", "0") or "0") > 0.5,
            "assignedFarm": f.get("ownedByFarmId", ""),
        })

    vehicles = []
    for v in root.findall(".//Vehicle"):
        vehicles.append({
            "vehicleName": v.get("name", ""),
            "vehicleType": v.get("type", ""),
            "category": v.get("category", ""),
            "status": "active" if v.get("controllerName") else "idle",
            "operator": v.get("controllerName", ""),
            "fillTypes": v.get("fillTypes", ""),
            "fillLevels": v.get("fillLevels", ""),
            "location": f"{v.get('x','0')},{v.get('z','0')}",
        })

    return {
        "serverName": server_name,
        "mapName": map_name,
        "playersOnline": players_online,
        "playerSlots": player_slots,
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


def _fetch_server_data(server: ServerConfig, timeout: int, retry_attempts: int) -> Dict[str, Any]:
    urls = _SERVER_FEEDS.get(server.server_id)
    if urls is None:
        logger.error("No feed mapping for server_id=%s", server.server_id)
        return {}

    stats_root = fetch_http_xml(urls["stats"], timeout=timeout, retry_attempts=retry_attempts)
    career_root = fetch_http_xml(urls["career"], timeout=timeout, retry_attempts=retry_attempts)

    if stats_root is None:
        logger.error("Stats feed unavailable for server %s", server.server_id)
        return {}

    payload = _parse_stats_xml(stats_root, server.name)
    farm_ids_hint = payload.pop("_farmIdsFromFarmlands", {})

    farms = _parse_career_xml(career_root, farm_ids_hint) if career_root is not None else []
    payload["farms"] = farms
    payload["farmIdsFromFarmlands"] = farm_ids_hint

    return payload


def _sync_to_base44(data: Dict[str, Any], api_key: str) -> None:
    server_name = data.get("serverName", "")
    session = _api_session(api_key)

    try:
        # 1. ServerStatus
        _upsert(session, "ServerStatus", "serverName", server_name, {
            "serverName": server_name,
            "mapName": data.get("mapName", ""),
            "playersOnline": data.get("playersOnline", 0),
            "playerSlots": data.get("playerSlots", 16),
            "currentTime": data.get("currentTime", ""),
            "lastUpdated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

        # 2. Farms
        fl_by_owner = data.get("farmIdsFromFarmlands", {})
        for farm in data.get("farms", []):
            fid = str(farm.get("farmId", ""))
            fl_info = fl_by_owner.get(fid, {"area": 0.0, "count": 0})
            farm_key = f"{server_name}::farm::{fid}"
            _upsert(session, "Farm", "farmId", farm_key, {
                "farmId": farm_key,
                "farmName": farm.get("farmName", ""),
                "color": str(farm.get("color", "0")),
                "balance": farm.get("balance", 0),
                "loan": farm.get("loan", 0),
                "serverName": server_name,
                "workedHectares": round(fl_info["area"], 2),
                "farmlandCount": fl_info["count"],
            })

        # 3. Fields (bulk replace — too many to diff)
        _bulk_replace(session, "Field", server_name, [
            {
                "fieldId": f.get("fieldId", ""),
                "fruitType": f.get("fruitType", ""),
                "growthState": f.get("growthState", 0),
                "isOwned": f.get("isOwned", False),
                "harvestReady": f.get("harvestReady", False),
                "weedState": f.get("weedState", 0),
                "sprayLevel": f.get("sprayLevel", 0),
                "limeLevel": f.get("limeLevel", 0),
                "plowLevel": f.get("plowLevel", 0),
                "needsAttention": f.get("needsAttention", False),
                "assignedFarm": f.get("assignedFarm", ""),
            }
            for f in data.get("fields", [])
        ])

        # 4. Vehicles (bulk replace — up to 150)
        _bulk_replace(session, "Vehicle", server_name, [
            {
                "vehicleName": v.get("vehicleName", ""),
                "vehicleType": v.get("vehicleType", ""),
                "category": v.get("category", ""),
                "status": v.get("status", "idle"),
                "operator": v.get("operator", ""),
                "fillTypes": v.get("fillTypes", ""),
                "fillLevels": v.get("fillLevels", ""),
                "location": v.get("location", ""),
            }
            for v in data.get("vehicles", [])[:150]
        ])

        print(f"\n✓ {server_name} fully synced.")

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

        _sync_to_base44(data, server.base44_api_key)

        logger.info("Done with server %s.", server.server_id)


def fetch_server_data(server_id: int) -> Dict[str, Any]:
    config = Config()
    server = config.get_servers(selected_server=server_id)[0]
    return _fetch_server_data(server, config.request_timeout, config.retry_attempts)

