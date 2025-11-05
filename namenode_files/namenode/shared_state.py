# namenode/shared_state.py
import threading
import json
import time
import os

# Load configuration once
try:
    CONFIG = json.load(open('../config.json'))
except FileNotFoundError:
    print("[ERROR] config.json not found. Exiting.")
    exit(1)

DATANODES = CONFIG['datanodes']
METADATA_FILE = "namenode_metadata.json"

# --- CORE SHARED RESOURCES ---

# 1. Lock for thread safety on all shared resources
METADATA_LOCK = threading.Lock()

# 2. Metadata (Chunk locations, File info)
def load_metadata():
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, "r") as f:
                data = f.read().strip()
                return json.loads(data) if data else {}
        except Exception as e:
            print(f"[WARN] Could not load metadata: {e}. Starting fresh.")
            return {}
    return {}

metadata = load_metadata()

# 3. Heartbeat Status (Last check-in time of Datanodes)
# Initialize all Datanodes as "alive" at the start time
datanode_heartbeat = {node['name']: 0 for node in DATANODES}

# 4. List of currently inactive nodes (managed by heartbeat_monitor)
INACTIVE_NODES = set()