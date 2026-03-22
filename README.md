# fs25_farm_bridge

Bridge Farming Simulator 25 save/live data from one or more GPortal servers into Base44.

## Multi-server setup (two farms)

Set per-server FTP credentials and optional per-server Base44 credentials:

```bash
GPORTAL_FTP_HOST_1=144.126.153.108
GPORTAL_FTP_PORT_1=51051
GPORTAL_FTP_USER_1=your_user_1
GPORTAL_FTP_PASS_1=your_pass_1
SERVER_NAME_1="KAW's farming playground 1"

GPORTAL_FTP_HOST_2=144.126.153.108
GPORTAL_FTP_PORT_2=51061
GPORTAL_FTP_USER_2=your_user_2
GPORTAL_FTP_PASS_2=your_pass_2
SERVER_NAME_2="KAW's farming playground 2"

# Option A: shared Base44 target for all servers
BASE44_API_URL=https://your-base44-url
BASE44_API_KEY=your-base44-key

# Option B: per-server Base44 targets
# BASE44_API_URL_1=...
# BASE44_API_KEY_1=...
# BASE44_API_URL_2=...
# BASE44_API_KEY_2=...
```

Optional per-server state cache files:

```bash
CACHE_FILE_1=.cache/server1_state.json
CACHE_FILE_2=.cache/server2_state.json
```

If you do not set `*_1` variables, the bridge runs in legacy single-server mode using:
`GPORTAL_FTP_HOST`, `GPORTAL_FTP_PORT`, `GPORTAL_FTP_USER`, `GPORTAL_FTP_PASS`,
`BASE44_API_URL`, `BASE44_API_KEY`, and `CACHE_FILE`.

## Run

```bash
# Sync default server (legacy mode) or first configured server
PYTHONPATH=src python -m fs25_farm_bridge

# Sync only one configured server
PYTHONPATH=src python -m fs25_farm_bridge --server 1

# Sync both/all configured servers
PYTHONPATH=src python -m fs25_farm_bridge --all
```