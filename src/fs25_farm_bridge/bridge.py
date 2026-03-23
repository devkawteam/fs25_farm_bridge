import logging
from typing import Dict, List, Optional

from .base44 import Base44Client
from .config import Config, ServerConfig
from .state import BridgeState
from .utils import (
    fetch_ftp_file,
    merge_data,
    parse_economy,
    parse_environment,
    parse_farms,
    parse_fields,
    parse_players,
    parse_xml,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GPortal FTP paths — adjust to your server layout if needed
# ---------------------------------------------------------------------------
_DEDICATED_STATS = "/gameserver/dedicated_server_stats.xml"
_CAREER_SAVEGAME = "/gameserver/savegame/careerSavegame.xml"
_FARMS_FILE = "/gameserver/savegame/farms.xml"
_FIELDS_FILE = "/gameserver/savegame/fields.xml"
_ECONOMY_FILE = "/gameserver/savegame/economy.xml"
_PLAYERS_FILE = "/gameserver/savegame/serverPlayers.xml"


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
            _sync(server, state, client)
        finally:
            state.save()
            client.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch(server: ServerConfig, path: str) -> Optional[bytes]:
    return fetch_ftp_file(
        host=server.ftp_host,
        port=server.ftp_port,
        user=server.ftp_user,
        password=server.ftp_pass,
        path=path,
    )


def _sync(server: ServerConfig, state: BridgeState, client: Base44Client) -> None:
    """Fetch all data sources, detect changes, and push updates to Base44."""

    # -- Environment (merge live stats + savegame) --
    live_bytes = _fetch(server, _DEDICATED_STATS)
    live_root = parse_xml(live_bytes) if live_bytes else None
    live_env = parse_environment(live_root) if live_root is not None else {}

    savegame_bytes = _fetch(server, _CAREER_SAVEGAME)
    savegame_root = parse_xml(savegame_bytes) if savegame_bytes else None
    savegame_env = parse_environment(savegame_root) if savegame_root is not None else {}

    environment = merge_data(live_env, savegame_env)
    if environment:
        environment["serverId"] = server.server_id
        environment["serverName"] = server.name

    # -- Farms --
    farms_bytes = _fetch(server, _FARMS_FILE)
    farms_root = parse_xml(farms_bytes) if farms_bytes else None
    farms = parse_farms(farms_root) if farms_root is not None else []
    farms = [
        {
            **farm,
            "serverId": server.server_id,
            "serverName": server.name,
        }
        for farm in farms
    ]

    # -- Fields --
    fields_bytes = _fetch(server, _FIELDS_FILE)
    fields_root = parse_xml(fields_bytes) if fields_bytes else None
    fields = parse_fields(fields_root) if fields_root is not None else []
    fields = [
        {
            **field,
            "serverId": server.server_id,
            "serverName": server.name,
        }
        for field in fields
    ]

    # -- Economy --
    economy_bytes = _fetch(server, _ECONOMY_FILE)
    economy_root = parse_xml(economy_bytes) if economy_bytes else None
    economy = parse_economy(economy_root) if economy_root is not None else {}
    if economy:
        economy["serverId"] = server.server_id
        economy["serverName"] = server.name

    # -- Players --
    players_bytes = _fetch(server, _PLAYERS_FILE)
    players_root = parse_xml(players_bytes) if players_bytes else None
    players = parse_players(players_root) if players_root is not None else []
    players = [
        {
            **player,
            "serverId": server.server_id,
            "serverName": server.name,
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
