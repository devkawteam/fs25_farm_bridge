# fs25_farm_bridge

Bridge Farming Simulator 25 save/live data from one or more GPortal servers into Base44.

Data is fetched exclusively via the FS25 HTTP Web API feed.

## Environment variables

```bash
# Shared Base44 API key (required)
BASE44_API_KEY=your-base44-key

# Optional: override the Base44 entity endpoint (defaults to FarmerProfile entity URL)
# BASE44_API_URL=https://api.base44.com/api/apps/<app-id>/entities/FarmerProfile

# Optional: override feed base URLs and access codes (built-in defaults below)
# FS25_FEED_BASE_URL_1=http://144.126.158.162:9120/feed
# FS25_FEED_CODE_1=mt3bqE0kBPlcS8Ld
# FS25_FEED_BASE_URL_2=http://144.126.153.108:9110/feed
# FS25_FEED_CODE_2=Axa2ixzvN7Gj8bQ3

# Optional: override server display names
# SERVER_NAME_1="KAW's farming playground 1"
# SERVER_NAME_2="KAW's farming playground 2"

# Optional: override per-server state cache paths
# CACHE_FILE_1=.cache/server1_state.json
# CACHE_FILE_2=.cache/server2_state.json
```

## Run

```bash
# Sync server 1
PYTHONPATH=src python -m fs25_farm_bridge --server 1

# Sync server 2
PYTHONPATH=src python -m fs25_farm_bridge --server 2

# Sync both servers
PYTHONPATH=src python -m fs25_farm_bridge --all
```