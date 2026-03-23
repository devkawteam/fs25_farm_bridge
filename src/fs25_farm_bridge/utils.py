import logging
import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def fetch_http_xml(
    url: str,
    timeout: int = 15,
    retry_attempts: int = 3,
) -> Optional[ET.Element]:
    """Fetch an XML document over HTTP and return its root element."""
    for attempt in range(1, retry_attempts + 1):
        try:
            response = requests.get(url, timeout=timeout)
            logger.info("GET %s -> %s", url, response.status_code)
            response.raise_for_status()
            root = parse_xml(response.content)
            if root is None:
                logger.error("HTTP XML parse failure for %s", url)
            return root
        except requests.exceptions.Timeout:
            logger.warning("HTTP timeout for %s on attempt %d/%d", url, attempt, retry_attempts)
        except requests.exceptions.RequestException as exc:
            logger.warning(
                "HTTP request error for %s on attempt %d/%d: %s",
                url,
                attempt,
                retry_attempts,
                exc,
            )

        if attempt < retry_attempts:
            time.sleep(2**attempt)

    logger.error("Exhausted retries while fetching %s", url)
    return None


# ---------------------------------------------------------------------------
# XML parsing
# ---------------------------------------------------------------------------

def parse_xml(data: bytes) -> Optional[ET.Element]:
    """Parse XML bytes and return the root element, or None on failure."""
    try:
        return ET.fromstring(data)
    except ET.ParseError as exc:
        logger.error("XML parse error: %s", exc)
        return None


def parse_environment(root: ET.Element) -> Dict[str, Any]:
    env: Dict[str, Any] = {}

    season_el = root.find(".//season")
    if season_el is not None:
        env["season"] = season_el.get("currentSeason", "unknown")
        env["day"] = season_el.get("currentDay", "0")

    weather_el = root.find(".//weather")
    if weather_el is not None:
        env["weather"] = weather_el.get("stateString", "unknown")

    time_el = root.find(".//time")
    if time_el is not None:
        env["timeScale"] = time_el.get("timeScale", "1")
        env["dayTime"] = time_el.get("dayTime", "0")

    return env


def parse_farms(root: ET.Element) -> List[Dict[str, Any]]:
    farms = []
    for farm_el in root.findall(".//farm"):
        money = farm_el.get("money", "0")
        farms.append(
            {
                "farmId": farm_el.get("farmId"),
                "name": farm_el.get("name", ""),
                "farmName": farm_el.get("name", ""),
                "money": money,
                "balance": money,
                "loan": farm_el.get("loan", "0"),
                "color": farm_el.get("color", "0"),
            }
        )
    return farms


def parse_fields(root: ET.Element) -> List[Dict[str, Any]]:
    fields = []
    for field_el in root.findall(".//field"):
        fields.append(
            {
                "fieldId": field_el.get("fieldId"),
                "fruitType": field_el.get("fruitType", ""),
                "growthState": field_el.get("growthState", "0"),
                "owned": field_el.get("owned", "false"),
                "ownedByFarmId": field_el.get("ownedByFarmId", ""),
                "soilState": field_el.get("newSoilState", "0"),
            }
        )
    return fields


def parse_economy(root: ET.Element) -> Dict[str, Any]:
    economy: Dict[str, Any] = {}

    stats_el = root.find(".//statistics")
    if stats_el is not None:
        economy["income"] = stats_el.get("income", "0")
        economy["expenses"] = stats_el.get("expenses", "0")

    prices = []
    for fill_el in root.findall(".//fillType"):
        prices.append(
            {
                "name": fill_el.get("name", ""),
                "price": fill_el.get("price", "0"),
            }
        )
    economy["prices"] = prices
    return economy


def parse_players(root: ET.Element) -> List[Dict[str, Any]]:
    players = []
    for player_el in root.findall(".//player"):
        nickname = player_el.get("nickname", player_el.get("lastNickname", ""))
        play_time = player_el.get("playTime", player_el.get("playTimeHours", "0"))
        players.append(
            {
                "uniqueUserId": player_el.get("uniqueUserId"),
                "farmId": player_el.get("farmId"),
                "name": nickname,
                "nickname": nickname,
                "stats": {"playTime": play_time},
                "isAdmin": player_el.get("isAdmin", "false"),
                "isOnline": player_el.get("isOnline", "false"),
            }
        )
    return players


def parse_vehicles(root: ET.Element) -> List[Dict[str, Any]]:
    vehicles: List[Dict[str, Any]] = []
    for vehicle_el in root.findall(".//vehicle"):
        vehicles.append(
            {
                "vehicleId": vehicle_el.get("id", ""),
                "farmId": vehicle_el.get("farmId", ""),
                "name": vehicle_el.get("filename", vehicle_el.get("name", "")),
                "operatingTime": vehicle_el.get("operatingTime", "0"),
            }
        )
    return vehicles


def parse_server_name(root: ET.Element) -> str:
    server_el = root.find(".//server")
    if server_el is not None:
        return server_el.get("name", server_el.get("serverName", ""))
    return root.get("serverName", "")


def merge_by_key(
    live_items: List[Dict[str, Any]],
    save_items: List[Dict[str, Any]],
    key: str,
) -> List[Dict[str, Any]]:
    """Merge list entities by key with live data taking precedence."""
    merged: Dict[str, Dict[str, Any]] = {}

    for item in save_items:
        item_key = str(item.get(key) or "")
        if item_key:
            merged[item_key] = dict(item)

    for item in live_items:
        item_key = str(item.get(key) or "")
        if not item_key:
            continue
        if item_key in merged:
            merged[item_key] = merge_data(item, merged[item_key])
        else:
            merged[item_key] = dict(item)

    return list(merged.values())


# ---------------------------------------------------------------------------
# Merging
# ---------------------------------------------------------------------------

def merge_data(
    live_data: Dict[str, Any],
    savegame_data: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Merge savegame and live data dictionaries.
    Live data takes precedence for shared keys; for lists the live version
    replaces the savegame version entirely.
    """
    merged = dict(savegame_data)
    for key, value in live_data.items():
        if key in merged and isinstance(merged[key], list) and isinstance(value, list):
            merged[key] = value
        else:
            merged[key] = value
    return merged
