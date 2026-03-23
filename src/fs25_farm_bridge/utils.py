import ftplib
import io
import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# FTP
# ---------------------------------------------------------------------------

def fetch_ftp_file(
    host: str,
    port: int,
    user: str,
    password: str,
    path: str,
) -> Optional[bytes]:
    """Download a single file from an FTP server. Returns raw bytes or None."""
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(host, port, timeout=30)
            ftp.login(user, password)
            buf = io.BytesIO()
            ftp.retrbinary(f"RETR {path}", buf.write)
            logger.debug("FTP: fetched '%s' (%d bytes)", path, buf.tell())
            return buf.getvalue()
    except ftplib.all_errors as exc:
        logger.error("FTP error fetching '%s': %s", path, exc)
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
        nickname = player_el.get("lastNickname", "")
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
