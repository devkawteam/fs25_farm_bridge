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

# ServerHub app (kawsplayground.online) — single destination
_SERVERHUB_APP_ID = "69ac6dca5af2dc4d433b68bd"
_SERVERHUB_ENTITY_URL = (
    f"https://api.base44.com/api/apps/{_SERVERHUB_APP_ID}/entities/FS25SaveData"
)

_PLAYGROUND_MAP = {
    "kaw's farming playground 1": "pg1",
    "kaw's farming playground 2": "pg2",
}


def _daytime_to_hhmm(day_time_ms: int) -> str:
    total_seconds = day_time_ms // 1000
    hours = (total_seconds // 3600) % 24
    minutes = (total_seconds % 3600) // 60
    return f"{hours:02d}:{minutes:02d}"


def _parse_stats_xml(root: Any, server_name: str) -> Dict[str, Any]:
    map_name = root.get("mapName", "")
    day_time_ms = int(root.get("dayTime", "0"))
    current_time = _daytime_to_hhmm(day_time_ms)

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

    farms = _parse_career_xml(career_root, farm_ids_hint) if career_root is not None else [
        {"farmId": fid, "farmName": f"Farm {fid}", "balance": 0, "loan": 0, "color": "0", "players": []}
        for fid in farm_ids_hint
    ]
    payload["farms"] = farms

    payload["economy"] = _parse_economy_xml(economy_root) if economy_root is not None else {}
    payload["environment"] = {
        "currentDay": 0,
        "currentTime": current_time,
        "season": "",
        "currentWeather": "",
        "timeSinceLastRain": 0,
        "forecast": [],
    }

    return payload


def _update_server_hub(data: Dict[str, Any], api_key: str) -> bool:
    """Write live server data to FS25SaveData in the ServerHub app."""
    server_name = (data.get("serverName") or "").lower()
    playground = _PLAYGROUND_MAP.get(server_name)
    if not playground:
        print(f"[HUB] Unknown server name '{server_name}', skipping.")
        return False

    session = requests.Session()
    session.headers.update({
        "api_key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    })

    try:
        # Find existing record
        resp = session.get(_SERVERHUB_ENTITY_URL, params={"playground": playground}, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        records = payload if isinstance(payload, list) else payload.get("records", [])
        record_id = records[0]["id"] if records else None

        # Crop prices
        crop_prices = []
        for crop, info in (data.get("economy", {}).get("cropPrices") or {}).items():
            history = info.get("priceHistory", {})
            price_per_liter = next(iter(history.values()), 0)
            price_ton = round(price_per_liter * 1000)
            if price_ton > 0:
                crop_prices.append({"name": crop, "price": price_ton, "factor": 1})

        # Farms with farmland stats
        farmland_by_owner: Dict[str, Dict] = {}
        for fl in data.get("farmlands", []):
            owner = str(fl.get("owner", "0"))
            if owner and owner != "0":
                if owner not in farmland_by_owner:
                    farmland_by_owner[owner] = {"area": 0.0, "count": 0}
                farmland_by_owner[owner]["area"] += float(fl.get("area", 0) or 0)
                farmland_by_owner[owner]["count"] += 1

        hub_farms = []
        for farm in data.get("farms", []):
            fid = str(farm.get("farmId", ""))
            fl_info = farmland_by_owner.get(fid, {"area": 0.0, "count": 0})
            hub_farms.append({
                "farm_id": fid,
                "farm_name": farm.get("farmName", f"Farm {fid}"),
                "balance": farm.get("balance", 0),
                "loan": farm.get("loan", 0),
                "total_area_ha": round(fl_info["area"], 2),
                "farmland_count": fl_info["count"],
            })

        # Vehicles
        hub_vehicles = []
        for idx, v in enumerate(data.get("vehicles", [])[:100]):
            loc = (v.get("location") or "0,0").split(",")
            hub_vehicles.append({
                "vehicle_id": str(idx + 1),
                "farm_id": "0",
                "type_name": v.get("vehicleName", ""),
                "category": (v.get("category") or "").lower(),
                "damage": 0,
                "wear": 0,
                "pos_x": float(loc[0]) if loc else 0.0,
                "pos_z": float(loc[1]) if len(loc) > 1 else 0.0,
                "needs_repair": False,
            })

        map_url = (
            "http://144.126.158.162:9120/feed/dedicated-server-stats-map.jpg?code=mt3bqE0kBPlcS8Ld&quality=60&size=512"
            if playground == "pg1"
            else "http://144.126.153.108:9110/feed/dedicated-server-stats-map.jpg?code=Axa2ixzvN7Gj8bQ3&quality=60&size=512"
        )

        env = data.get("environment", {})
        hub_data = {
            "playground": playground,
            "server_online": True,
            "server_name": data.get("map", ""),
            "map_name": data.get("map", ""),
            "map_url": map_url,
            "in_game_time": env.get("currentTime", ""),
            "ingame_day": env.get("currentDay") or None,
            "ingame_season": env.get("season") or None,
            "player_count": len(data.get("players", [])),
            "online_players": [
                {"name": p.get("name"), "uptime": p.get("uptime"), "isAdmin": p.get("isAdmin")}
                for p in data.get("players", [])
            ],
            "vehicle_count": len(data.get("vehicles", [])),
            "vehicles": hub_vehicles,
            "field_count": len(data.get("fields", [])),
            "farm_count": len(hub_farms),
            "farms": hub_farms,
            "crop_prices": crop_prices,
            "synced_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "sync_status": "ok",
        }

        if record_id:
            resp2 = session.put(f"{_SERVERHUB_ENTITY_URL}/{record_id}", json=hub_data, timeout=15)
            resp2.raise_for_status()
            print(f"[HUB] ✓ Updated FS25SaveData ({playground})")
        else:
            resp2 = session.post(_SERVERHUB_ENTITY_URL, json=hub_data, timeout=15)
            resp2.raise_for_status()
            print(f"[HUB] ✓ Created FS25SaveData ({playground})")

        return True

    except Exception as exc:
        print(f"[HUB] ✗ Failed ({playground}): {exc}")
        logger.error("ServerHub update failed: %s", exc)
        return False
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

        print(f"  Map: {data.get('map', '?')}")
        print(f"  Farms: {len(data.get('farms', []))}")
        print(f"  Fields: {len(data.get('fields', []))}")
        print(f"  Vehicles: {len(data.get('vehicles', []))}")
        print(f"  Players online: {data.get('playersOnline', 0)}")

        _update_server_hub(data, server.base44_api_key)

        logger.info("Done with server %s.", server.server_id)


def fetch_server_data(server_id: int) -> Dict[str, Any]:
    config = Config()
    server = config.get_servers(selected_server=server_id)[0]
    return _fetch_server_data(server, config.request_timeout, config.retry_attempts)
