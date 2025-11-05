import socket
import threading
import os
import json
import time
import sys

# --- Configuration and Initialization ---
# Add a lock for thread-safe access to Datanode's local metadata
METADATA_LOCK = threading.Lock()

CONFIG = json.load(open('../config.json'))

if len(sys.argv) < 2:
    print("Usage: python3 datanode.py <datanode_id>")
    sys.exit(1)

DATANODE_ID = int(sys.argv[1])

try:
    MY_CONFIG = CONFIG['datanodes'][DATANODE_ID]
except IndexError:
    print(f"Error: Datanode ID {DATANODE_ID} is out of range.")
    sys.exit(1)

STORAGE_DIR = f"storage_{MY_CONFIG['name']}/"
os.makedirs(STORAGE_DIR, exist_ok=True)

METADATA_FILE = f"metadata_{MY_CONFIG['name']}.json"

NAMENODE_HOST = CONFIG['namenode']['host']
HEARTBEAT_PORT = CONFIG['namenode']['port'] + 1

# --- Global In-Memory Metadata (Protected by Lock) ---
datanode_metadata = {} 

# ------------------------------
# Metadata helpers
# ------------------------------
def load_metadata():
    """Loads metadata from disk into the global variable."""
    global datanode_metadata
    if os.path.exists(METADATA_FILE):
        try:
            datanode_metadata = json.load(open(METADATA_FILE))
        except Exception:
            datanode_metadata = {}
    else:
        datanode_metadata = {}

def save_metadata():
    """Saves the global metadata dictionary to disk."""
    try:
        with open(METADATA_FILE, "w") as f:
            json.dump(datanode_metadata, f, indent=4)
    except Exception as e:
        print(f"[ERROR] Failed to save metadata: {e}")


def update_chunk_metadata(filename, chunk_idx, chunk_size):
    """Updates chunk metadata in the global variable and saves to disk."""
    with METADATA_LOCK:
        chunk_name = f"{filename}_part{chunk_idx}"
        datanode_metadata[chunk_name] = {
            "filename": filename,
            "chunk_index": int(chunk_idx),
            "chunk_size": int(chunk_size),
            "path": os.path.join(STORAGE_DIR, chunk_name),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        save_metadata()

# Load initial metadata on startup
load_metadata()

# ------------------------------
# Heartbeat Thread
# ------------------------------
def send_heartbeat():
    while True:
        try:
            s = socket.socket()
            s.connect((NAMENODE_HOST, HEARTBEAT_PORT))
            msg = json.dumps({
                "node": MY_CONFIG['name'],
                "host": MY_CONFIG['host'],
                "port": MY_CONFIG['port'],
                "status": "alive"
            })
            s.sendall(msg.encode())
            s.close()
            print(f"[HEARTBEAT] Sent heartbeat to {NAMENODE_HOST}:{HEARTBEAT_PORT}") # Uncomment for verbose
        except Exception as e:
            print(f"[ERROR] Heartbeat failed: {e}. Is the Namenode running?")
        time.sleep(5)

# ------------------------------
# Handle STORE / RETRIEVE
# ------------------------------
def handle_request(conn):
    try:
        # --- 1. Receive and Parse Header ---
        header = b""
        # Read byte by byte until the newline to ensure we get the full command header
        while not header.endswith(b"\n"):
            part = conn.recv(1)
            if not part:
                conn.close()
                return
            header += part

        header = header.decode().strip()
        parts = header.split(":")
        msg_type = parts[0]

        # --- STORE LOGIC ---
        if msg_type == "STORE" and len(parts) == 4:
            _, filename, chunk_idx, chunk_size_str = parts
            chunk_size = int(chunk_size_str)
            
            # Send READY to Namenode/Client to start receiving the payload
            conn.send(b"READY")

            data = b""
            # Receive data until the <END> marker
            while True:
                # Use a larger buffer (4096) for faster reception
                chunk = conn.recv(4096) 
                if not chunk:
                    break
                
                # Check for the end marker. This is where the Namenode sends the data + <END>
                if b"<END>" in chunk:
                    data += chunk.replace(b"<END>", b"")
                    break
                data += chunk

            # Write chunk to disk
            chunk_name = f"{filename}_part{chunk_idx}"
            chunk_path = os.path.join(STORAGE_DIR, chunk_name)
            
            if len(data) != chunk_size:
                 raise Exception(f"Size mismatch for {chunk_name}. Expected {chunk_size}, got {len(data)}")

            with open(chunk_path, "wb") as f:
                f.write(data)

            update_chunk_metadata(filename, chunk_idx, len(data))
            print(f"[{MY_CONFIG['name']} STORED] {chunk_name} ({len(data)} bytes)")
            conn.send(b"STORED")

        # --- RETRIEVE LOGIC (For Client Download / Namenode Re-replication) ---
        elif msg_type == "RETRIEVE" and len(parts) == 3:
            _, filename, chunk_idx = parts
            chunk_name = f"{filename}_part{chunk_idx}"
            chunk_path = os.path.join(STORAGE_DIR, chunk_name)

            if os.path.exists(chunk_path):
                with open(chunk_path, "rb") as f:
                    data = f.read()
                
                # CRITICAL FIX: Send size header expected by Namenode
                size_header = f"SIZE:{len(data)}\n".encode()
                conn.sendall(size_header)
                
                # Wait for Namenode to send 'READY' after reading the size
                if conn.recv(16) == b"READY":
                    conn.sendall(data)
                    print(f"[{MY_CONFIG['name']} SENT] {chunk_name} ({len(data)} bytes)")
                else:
                    print(f"[WARN] NameNode didn't send READY after size header.")
            else:
                conn.send(b"NOT_FOUND\n") # Send clear NOT_FOUND response
                print(f"[WARN] Requested chunk {chunk_name} not found.")
                
        else:
            print(f"[WARN] Invalid header received: {header}")

    except Exception as e:
        print(f"[ERROR] Request handler failed on {MY_CONFIG['name']}: {e}")
    finally:
        conn.close()

# ------------------------------
# Listener
# ------------------------------
def listen_for_chunks():
    s = socket.socket()
    s.bind((MY_CONFIG['host'], MY_CONFIG['port']))
    s.listen(5)
    print(f"[DATANODE {MY_CONFIG['name']}] Listening on {MY_CONFIG['host']}:{MY_CONFIG['port']}")
    while True:
        conn, addr = s.accept()
        # Spawn a new thread to handle the incoming request (STORE or RETRIEVE)
        threading.Thread(target=handle_request, args=(conn,), daemon=True).start()

# ------------------------------
# Main
# ------------------------------
if __name__ == "__main__":
    # Start the continuous heartbeat in a daemon thread
    threading.Thread(target=send_heartbeat, daemon=True).start()
    
    # Start the listener thread to wait for commands from Namenode/Client
    listen_for_chunks()