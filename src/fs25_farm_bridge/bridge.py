import logging
from typing import List, Optional

from .base44 import Base44Client
from .config import Config
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

def run() -> None:
    """Top-level bridge run: fetch → parse → diff → publish."""
    config = Config()
    state = BridgeState(config.cache_file)
    client = Base44Client(
        api_url=config.base44_api_url,
        api_key=config.base44_api_key,
        timeout=config.request_timeout,
        retry_attempts=config.retry_attempts,
    )

    try:
        _sync(config, state, client)
    finally:
        state.save()
        client.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch(config: Config, path: str) -> Optional[bytes]:
    return fetch_ftp_file(
        host=config.ftp_host,
        port=config.ftp_port,
        user=config.ftp_user,
        password=config.ftp_pass,
        path=path,
    )


def _sync(config: Config, state: BridgeState, client: Base44Client) -> None:
    """Fetch all data sources, detect changes, and push updates to Base44."""

    # -- Environment (merge live stats + savegame) --
    live_bytes = _fetch(config, _DEDICATED_STATS)
    live_root = parse_xml(live_bytes) if live_bytes else None
    live_env = parse_environment(live_root) if live_root is not None else {}

    savegame_bytes = _fetch(config, _CAREER_SAVEGAME)
    savegame_root = parse_xml(savegame_bytes) if savegame_bytes else None
    savegame_env = parse_environment(savegame_root) if savegame_root is not None else {}

    environment = merge_data(live_env, savegame_env)

    # -- Farms --
    farms_bytes = _fetch(config, _FARMS_FILE)
    farms_root = parse_xml(farms_bytes) if farms_bytes else None
    farms = parse_farms(farms_root) if farms_root is not None else []

    # -- Fields --
    fields_bytes = _fetch(config, _FIELDS_FILE)
    fields_root = parse_xml(fields_bytes) if fields_bytes else None
    fields = parse_fields(fields_root) if fields_root is not None else []

    # -- Economy --
    economy_bytes = _fetch(config, _ECONOMY_FILE)
    economy_root = parse_xml(economy_bytes) if economy_bytes else None
    economy = parse_economy(economy_root) if economy_root is not None else {}

    # -- Players --
    players_bytes = _fetch(config, _PLAYERS_FILE)
    players_root = parse_xml(players_bytes) if players_bytes else None
    players = parse_players(players_root) if players_root is not None else []

    # -- Smart sync: only forward what changed --
    _publish_environment(state, client, environment)
    _publish_farms(state, client, farms)
    _publish_fields(state, client, fields)
    _publish_economy(state, client, economy)
    _publish_players(state, client, players)

    logger.info("Bridge sync complete.")


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
