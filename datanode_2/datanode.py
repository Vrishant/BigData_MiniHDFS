import socket
import threading
import os
import json
import time
import sys

# ------------------------------
# Load config
# ------------------------------
CONFIG = json.load(open('../config.json'))

# Choose DataNode by ID
if len(sys.argv) < 2:
    print("Usage: python3 datanode_c.py <datanode_id>")
    sys.exit(1)

DATANODE_ID = int(sys.argv[1])
MY_CONFIG = CONFIG['datanodes'][DATANODE_ID]

# Directories
STORAGE_DIR = f"storage_{MY_CONFIG['name']}/"
os.makedirs(STORAGE_DIR, exist_ok=True)

# Local metadata file
METADATA_FILE = f"metadata_{MY_CONFIG['name']}.json"
if not os.path.exists(METADATA_FILE):
    json.dump({}, open(METADATA_FILE, "w"))

# NameNode info
NAMENODE_HOST = CONFIG['namenode']['host']
HEARTBEAT_PORT = CONFIG['namenode']['port'] + 1


# ------------------------------
# Metadata helpers
# ------------------------------
def load_metadata():
    with open(METADATA_FILE, "r") as f:
        return json.load(f)


def save_metadata(metadata):
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=4)


def update_chunk_metadata(filename, chunk_idx, chunk_size, datanode_name):
    metadata = load_metadata()
    chunk_name = f"{filename}_part{chunk_idx}"

    metadata[chunk_name] = {
        "filename": filename,
        "chunk_index": int(chunk_idx),
        "chunk_size": int(chunk_size),
        "datanode": datanode_name,
        "path": os.path.join(STORAGE_DIR, chunk_name),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    save_metadata(metadata)


# ------------------------------
# Heartbeat Thread
# ------------------------------
def send_heartbeat():
    """Send heartbeat every 5 seconds to the NameNode"""
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
            s.send(msg.encode())
            s.close()
            print(f"[HEARTBEAT] Sent heartbeat to {NAMENODE_HOST}:{HEARTBEAT_PORT}")
        except Exception as e:
            print(f"[ERROR] Heartbeat failed: {e}")
        time.sleep(5)


# ------------------------------
# Handle STORE / RETRIEVE requests
# ------------------------------
def handle_request(conn):
    try:
        header = conn.recv(1024).decode()
        if not header:
            return

        parts = header.split(":")
        msg_type = parts[0]

        if msg_type == "STORE":
            if len(parts) != 4:
                print(f"[WARN] Invalid STORE header: {header}")
                conn.close()
                return

            _, filename, chunk_idx, chunk_size = parts
            chunk_size = int(chunk_size)

            # Acknowledge ready
            conn.send(b"READY")

            data = b""
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                if b"<END>" in chunk:
                    data += chunk.replace(b"<END>", b"")
                    break
                data += chunk

            chunk_name = f"{filename}_part{chunk_idx}"
            chunk_path = os.path.join(STORAGE_DIR, chunk_name)

            with open(chunk_path, "wb") as f:
                f.write(data)

            print(f"[STORED] {chunk_name} ({len(data)} bytes)")
            conn.send(b"STORED")

        elif msg_type == "RETRIEVE":
            if len(parts) != 3:
                print(f"[WARN] Invalid RETRIEVE header: {header}")
                conn.close()
                return

            _, filename, chunk_idx = parts
            chunk_name = f"{filename}_part{chunk_idx}"
            chunk_path = os.path.join(STORAGE_DIR, chunk_name)

            if os.path.exists(chunk_path):
                with open(chunk_path, "rb") as f:
                    data = f.read()
                conn.sendall(data + b"<END>")
                print(f"[SENT] {chunk_name}")
            else:
                conn.send(b"NOT_FOUND")

        else:
            print(f"[WARN] Unknown header: {header}")

    except Exception as e:
        print(f"[ERROR] Request handler: {e}")
    finally:
        conn.close()


# ------------------------------
# Listener Thread
# ------------------------------
def listen_for_chunks():
    """Listen for incoming STORE and RETRIEVE requests from NameNode"""
    s = socket.socket()
    s.bind((MY_CONFIG['host'], MY_CONFIG['port']))
    s.listen(5)
    print(f"[DATANODE {MY_CONFIG['name']}] Listening on {MY_CONFIG['host']}:{MY_CONFIG['port']}")
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_request, args=(conn,), daemon=True).start()


# ------------------------------
# Main
# ------------------------------
if __name__ == "__main__":
    threading.Thread(target=send_heartbeat, daemon=True).start()
    listen_for_chunks()