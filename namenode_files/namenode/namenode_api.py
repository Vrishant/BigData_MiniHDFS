# namenode/namenode_api.py
from flask import Flask, jsonify
import threading
import time
# Import the central shared state and the lock
from shared_state import datanode_heartbeat, DATANODES, CONFIG, METADATA_LOCK, INACTIVE_NODES
import namenode_c # Reuse core NameNode functions

app = Flask(__name__)

@app.route('/heartbeat_status')
def heartbeat_status():
    now = time.time()
    status = {}
    
    # Use the lock to safely read the heartbeat data
    with METADATA_LOCK:
        for node in DATANODES:
            name = node['name']
            last = datanode_heartbeat.get(name, 0)
            
            # Check if the node is currently marked inactive by the monitor
            is_down = name in INACTIVE_NODES 
            
            status[name] = {
                "last_heartbeat": time.ctime(last) if last > 0 else "Never Checked In",
                "status": "Offline (DOWN)" if is_down else "Online"
            }
            
    return jsonify(status)

@app.route('/file_metadata')
def file_metadata():
    # Use the lock to safely read the metadata
    with METADATA_LOCK:
        # Return a copy of the metadata to prevent external modification
        return jsonify(namenode_c.metadata) 

if __name__ == "__main__":
    # Start the core Namenode services
    threading.Thread(target=namenode_c.listen_for_client, daemon=True).start()
    threading.Thread(target=namenode_c.heartbeat_listener, daemon=True).start()
    # The monitor now handles re-replication and recovery
    threading.Thread(target=namenode_c.heartbeat_monitor, daemon=True).start() 
    
    # Flask runs the API on a separate port (e.g., 5002 if namenode is 5000)
    print(f"[API] Starting Flask API on port {CONFIG['namenode']['port'] + 2}")
    app.run(host=CONFIG['namenode']['host'], port=CONFIG['namenode']['port'] + 2, debug=False, use_reloader=False)