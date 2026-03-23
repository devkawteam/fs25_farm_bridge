import logging
from typing import Any, Dict, List, Optional

from .base44 import Base44Client
from .config import Config, ServerConfig
from .state import BridgeState
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


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(selected_server: Optional[int] = None, run_all: bool = False) -> None:
    """Top-level bridge run: fetch → parse → diff → publish."""
    config = Config()
    servers = config.get_servers(selected_server=selected_server, run_all=run_all)

    for server in servers:
        logger.info(
            "Starting sync for server %s (%s)",
            server.server_id,
            server.name,
        )

        state = BridgeState(server.cache_file)
        client = Base44Client(
            api_url=server.base44_api_url,
            api_key=server.base44_api_key,
            timeout=config.request_timeout,
            retry_attempts=config.retry_attempts,
        )

        try:
            _sync(
                server,
                state,
                client,
                timeout=config.request_timeout,
                retry_attempts=config.retry_attempts,
            )
        finally:
            state.save()
            client.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def fetch_server_data(server_id: int) -> Dict[str, Any]:
    """
    Fetch and merge live + savegame XML data for one configured server.
    """
    config = Config()
    server = config.get_servers(selected_server=server_id)[0]
    return _fetch_server_data(server, config.request_timeout, config.retry_attempts)


def _fetch_server_data(
    server: ServerConfig,
    timeout: int,
    retry_attempts: int,
) -> Dict[str, Any]:
    urls = _SERVER_FEEDS.get(server.server_id)
    if urls is None:
        logger.error("No HTTP feed mapping for server_id=%s", server.server_id)
        return {
            "serverName": server.name,
            "environment": {},
            "players": [],
            "farms": [],
            "fields": [],
            "vehicles": [],
            "economy": {},
        }

    stats_root = fetch_http_xml(urls["stats"], timeout=timeout, retry_attempts=retry_attempts)
    career_root = fetch_http_xml(urls["career"], timeout=timeout, retry_attempts=retry_attempts)
    economy_root = fetch_http_xml(urls["economy"], timeout=timeout, retry_attempts=retry_attempts)
    vehicles_root = fetch_http_xml(urls["vehicles"], timeout=timeout, retry_attempts=retry_attempts)

    return {
        "serverName": server.name,
        "environment": parse_environment(stats_root) if stats_root is not None else {},
        "players": parse_players(stats_root) if stats_root is not None else [],
        "farms": parse_farms(career_root) if career_root is not None else [],
        "fields": parse_fields(career_root) if career_root is not None else [],
        "vehicles": parse_vehicles(vehicles_root) if vehicles_root is not None else [],
        "economy": parse_economy(economy_root) if economy_root is not None else {},
    }


def _sync(
    server: ServerConfig,
    state: BridgeState,
    client: Base44Client,
    timeout: int,
    retry_attempts: int,
) -> None:
    """Fetch all data sources over HTTP, detect changes, and push updates to Base44."""

    snapshot = _fetch_server_data(
        server=server,
        timeout=timeout,
        retry_attempts=retry_attempts,
    )

    environment = snapshot.get("environment", {})
    if environment:
        environment["serverId"] = server.server_id
        environment["serverName"] = snapshot.get("serverName") or server.name

    farms = snapshot.get("farms", [])
    farms = [
        {
            **farm,
            "serverId": server.server_id,
            "serverName": snapshot.get("serverName") or server.name,
        }
        for farm in farms
    ]

    fields = snapshot.get("fields", [])
    fields = [
        {
            **field,
            "serverId": server.server_id,
            "serverName": snapshot.get("serverName") or server.name,
        }
        for field in fields
    ]

    economy = snapshot.get("economy", {})
    if economy:
        economy["serverId"] = server.server_id
        economy["serverName"] = snapshot.get("serverName") or server.name

    players = snapshot.get("players", [])
    players = [
        {
            **player,
            "serverId": server.server_id,
            "serverName": snapshot.get("serverName") or server.name,
        }
        for player in players
    ]

    # Build player lists on each farm so FarmerProfile mapping can stay farm-centric.
    players_by_farm: Dict[str, List[dict]] = {}
    for player in players:
        farm_id = str(player.get("farmId") or "")
        players_by_farm.setdefault(farm_id, []).append(player)
    farms_with_players = [
        {
            **farm,
            "players": players_by_farm.get(str(farm.get("farmId") or ""), []),
        }
        for farm in farms
    ]
    farmer_profiles = build_farmer_profiles(farms_with_players, server.name)

    # -- Smart sync: only forward what changed --
    _publish_environment(state, client, environment)
    _publish_farms(state, client, farms)
    _publish_fields(state, client, fields)
    _publish_economy(state, client, economy)
    _publish_players(state, client, players)
    _publish_farmer_profiles(state, client, farmer_profiles)

    logger.info(
        "Bridge sync complete for server %s (%s).",
        server.server_id,
        server.name,
    )


def _publish_environment(
    state: BridgeState, client: Base44Client, environment: dict
) -> None:
    if not environment:
        return
    if state.has_changed("environment", environment):
        logger.info("Environment changed — sending to Base44.")
        client.send_environment(environment)
    else:
        logger.debug("Environment unchanged — skipped.")


def _publish_farms(
    state: BridgeState, client: Base44Client, farms: List[dict]
) -> None:
    changed: List[dict] = [
        farm for farm in farms
        if state.has_changed(f"farm_{farm.get('farmId')}", farm)
    ]
    if changed:
        logger.info("%d farm(s) changed — sending to Base44.", len(changed))
        client.send_farms(changed)
    else:
        logger.debug("All farms unchanged — skipped.")


def _publish_fields(
    state: BridgeState, client: Base44Client, fields: List[dict]
) -> None:
    changed: List[dict] = [
        field for field in fields
        if state.has_changed(f"field_{field.get('fieldId')}", field)
    ]
    if changed:
        logger.info("%d field(s) changed — sending to Base44.", len(changed))
        client.send_fields(changed)
    else:
        logger.debug("All fields unchanged — skipped.")


def _publish_economy(
    state: BridgeState, client: Base44Client, economy: dict
) -> None:
    if not economy:
        return
    if state.has_changed("economy", economy):
        logger.info("Economy changed — sending to Base44.")
        client.send_economy(economy)
    else:
        logger.debug("Economy unchanged — skipped.")


def _publish_players(
    state: BridgeState, client: Base44Client, players: List[dict]
) -> None:
    if state.has_changed("players", players):
        logger.info("Players changed — sending to Base44.")
        client.send_players(players)
    else:
        logger.debug("Players unchanged — skipped.")


def build_farmer_profiles(farms: List[dict], server_name: str) -> List[dict]:
    """
    Build one FarmerProfile record per player from farm-oriented source data.
    """
    profiles: List[dict] = []

    for farm in farms:
        farm_name = farm.get("farmName") or farm.get("name") or ""
        money = farm.get("balance") if farm.get("balance") is not None else farm.get("money")
        loan = farm.get("loan", 0)
        farm_color = farm.get("color", "")
        farm_players = farm.get("players") or []

        for player in farm_players:
            profile = {
                "farm_name": farm_name,
                "money": money,
                "loan": loan,
                "ingame_name": player.get("nickname") or player.get("name") or "",
                "discord_id": str(player.get("uniqueUserId") or ""),
                "playtime_hours": (
                    (player.get("stats") or {}).get("playTime")
                    if isinstance(player.get("stats"), dict)
                    else player.get("playTime", 0)
                ),
                "farm_color": farm_color,
                "notify_server": server_name,
                "is_active": True,
            }

            if profile["discord_id"] and profile["ingame_name"]:
                profiles.append(profile)

    return profiles


def send_farmer_profiles(client: Base44Client, profiles: List[dict]) -> None:
    """
    Upsert FarmerProfile entities by (discord_id, notify_server).
    """
    try:
        existing_profiles = client.get_farmer_profiles()
    except Exception as exc:  # pragma: no cover - hard safety net
        logger.error("Errors fetching FarmerProfile entities: %s", exc)
        return

    index: Dict[tuple, dict] = {}
    for item in existing_profiles:
        key = (str(item.get("discord_id") or ""), str(item.get("notify_server") or ""))
        if key[0] and key[1]:
            index[key] = item

    for profile in profiles:
        key = (str(profile.get("discord_id") or ""), str(profile.get("notify_server") or ""))
        if not key[0] or not key[1]:
            logger.error("Errors: skipping profile with missing identity fields: %s", profile)
            continue

        existing = index.get(key)
        entity_id = (
            (existing or {}).get("id")
            or (existing or {}).get("_id")
            or (existing or {}).get("entityId")
        )

        try:
            if entity_id:
                ok = client.update_farmer_profile(str(entity_id), profile)
                if ok:
                    logger.info("Update: discord_id=%s server=%s", key[0], key[1])
                else:
                    logger.error("Errors: update failed for discord_id=%s server=%s", key[0], key[1])
                continue

            ok = client.create_farmer_profile(profile)
            if ok:
                logger.info("Create: discord_id=%s server=%s", key[0], key[1])
            else:
                logger.error("Errors: create failed for discord_id=%s server=%s", key[0], key[1])
        except Exception as exc:  # pragma: no cover - hard safety net
            logger.error("Errors sending FarmerProfile for discord_id=%s: %s", key[0], exc)


def _publish_farmer_profiles(
    state: BridgeState,
    client: Base44Client,
    farmer_profiles: List[dict],
) -> None:
    changed_profiles: List[dict] = []

    for profile in farmer_profiles:
        key = profile.get("discord_id")
        server = profile.get("notify_server")
        if not key or not server:
            continue
        if state.has_changed(f"farmer_profile_{server}_{key}", profile):
            changed_profiles.append(profile)

    if not changed_profiles:
        logger.debug("Farmer profiles unchanged — skipped.")
        return

    logger.info("%d FarmerProfile record(s) changed — syncing.", len(changed_profiles))
    send_farmer_profiles(client, changed_profiles)
