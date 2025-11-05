import socket
import json
import os
import threading
import time

CONFIG = json.load(open('./config.json'))
NAMENODE = CONFIG['namenode']
DATANODES = CONFIG['datanodes']
CLIENT = CONFIG['client']
CHUNK_SIZE = 2 * 1024 * 1024  # 2MB

UPLOAD_PROGRESS = {"percent": 0}
DOWNLOAD_PROGRESS = {"percent": 0}

# ------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------
def connect_to_node(ip, port):
    """Create socket connection."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    return s


def upload_to_namenode(filepath):
    """Send entire file to the Namenode as per new protocol."""
    try:
        filename = os.path.basename(filepath)
        filesize = os.path.getsize(filepath)

        print(f"[CLIENT] Connecting to Namenode {NAMENODE['host']}:{NAMENODE['port']}")
        s = connect_to_node(NAMENODE['host'], NAMENODE['port'])

        # Step 1: Announce upload
        s.send(f"UPLOAD:{filename}:{filesize}".encode())

        # Step 2: Wait for READY
        ack = s.recv(1024).decode()
        if ack != "READY":
            print(f"[ERROR] Namenode not ready → {ack}")
            s.close()
            return

        print(f"[CLIENT] Sending {filename} in chunks of {CHUNK_SIZE} bytes...")

        # Step 3: Send file data in chunks
        sent = 0
        with open(filepath, "rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                s.sendall(chunk)
                sent += len(chunk)
                UPLOAD_PROGRESS["percent"] = round((sent / filesize) * 100, 2)

        # Step 4: Send end marker
        s.sendall(b"<END>")

        # Step 5: Get confirmation
        response = s.recv(1024).decode()
        if response == "UPLOAD_SUCCESS":
            print(f"[CLIENT] Upload successful ✅ ({filename})")
        else:
            print(f"[WARN] Unexpected response from Namenode → {response}")

        s.close()

    except Exception as e:
        print(f"[ERROR] Upload failed: {e}")

def get_datanode_by_name(name):
    """Find Datanode config by name."""
    for node in DATANODES:
        if node['name'] == name:
            return node
    return None

# ------------------------------------------------------------
# Upload Logic
# ------------------------------------------------------------
# def upload_file(filepath):
#     """Send file directly to the Namenode for handling"""
#     try:
#         filename = os.path.basename(filepath)
#         filesize = os.path.getsize(filepath)
#         print(f"[CLIENT] Connecting to Namenode {NAMENODE['host']}:{NAMENODE['port']}")

#         # Connect to Namenode
#         s = connect_to_node(NAMENODE['host'], NAMENODE['port'])

#         # Send upload header
#         s.send(f"UPLOAD:{filename}:{filesize}".encode())
#         ack = s.recv(8).decode()

#         if ack != "READY":
#             print(f"[CLIENT] Namenode not ready (got {ack})")
#             s.close()
#             return

#         # Send file data to Namenode
#         with open(filepath, "rb") as f:
#             bytes_sent = 0
#             while True:
#                 chunk = f.read(4096)
#                 if not chunk:
#                     break
#                 s.sendall(chunk)
#                 bytes_sent += len(chunk)
#                 UPLOAD_PROGRESS["percent"] = round((bytes_sent / filesize) * 100, 2)

#         # Notify end of transmission
#         s.send(b"<END>")
#         print(f"[CLIENT] File sent to Namenode: {filename}")

#         response = s.recv(1024).decode()
#         print(f"[CLIENT] Namenode response: {response}")
#         s.close()

#         print("[CLIENT] Upload complete ✅")

#     except Exception as e:
#         print(f"[ERROR] Upload failed: {e}")
        
        
# ------------------------------------------------------------
# Download Logic
# ------------------------------------------------------------
def download_file(filename, output_path):
    """Request chunks from Datanodes and rebuild file."""
    assignments = metadata_request(filename)
    if not assignments:
        print("[CLIENT] Metadata not found for download.")
        return

    with open(output_path, "wb") as f:
        total_chunks = len(assignments)
        for i, assigned_nodes in enumerate(assignments):
            chunk_data = None
            for node in assigned_nodes:
                try:
                    node_cfg = get_datanode_by_name(node['name'])
                    s = connect_to_node(node_cfg['host'], node_cfg['port'])
                    s.send(f"RETRIEVE:{filename}:{i}".encode())
                    data = s.recv(CHUNK_SIZE)
                    s.close()
                    if data:
                        chunk_data = data
                        break
                except Exception as e:
                    print(f"[WARN] Could not get chunk {i} from {node['name']} → {e}")
            if chunk_data:
                f.write(chunk_data)
            DOWNLOAD_PROGRESS["percent"] = round(((i + 1) / total_chunks) * 100, 2)

    print("[CLIENT] Download complete ✅")

# ------------------------------------------------------------
# Metadata Helper
# ------------------------------------------------------------
def metadata_request(filename):
    """Ask Namenode for metadata (for download reconstruction)."""
    try:
        s = connect_to_node(NAMENODE['host'], NAMENODE['port'])
        s.send(f"CHUNK_META:{filename}".encode())
        resp = s.recv(4096).decode()
        s.close()
        return json.loads(resp)
    except Exception as e:
        print(f"[CLIENT ERROR] Metadata request failed → {e}")
        return None

# ------------------------------------------------------------
# For Flask Progress Endpoint
# ------------------------------------------------------------
def get_progress():
    return {"upload": UPLOAD_PROGRESS["percent"], "download": DOWNLOAD_PROGRESS["percent"]}

# ------------------------------------------------------------
# Run Tests
# ------------------------------------------------------------
# if __name__ == "__main__":
    # test_file = "../data/test.txt"
    # upload_file(test_file)
