import socket
import json
import os
import time
import requests # Added for NameNode API calls
import hashlib  # Added for data integrity checks

# --- Configuration and State ---
CONFIG = json.load(open('./config.json'))
NAMENODE = CONFIG['namenode']
DATANODES = CONFIG['datanodes']
CHUNK_SIZE = 2 * 1024 * 1024 # 2MB

# Global state for progress tracking
UPLOAD_PROGRESS = {"percent": 0.0, "status": "idle"}
DOWNLOAD_PROGRESS = {"percent": 0.0, "status": "idle"}

# ------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------
def connect_to_node(ip, port):
    """Create socket connection."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(10) # Set timeout for connection attempts
    s.connect((ip, port))
    return s

def calculate_checksum(data):
    """Calculate SHA-256 hash for data integrity."""
    return hashlib.sha256(data).hexdigest()

def get_datanode_by_name(name):
    """Find Datanode config by name."""
    for node in DATANODES:
        if node['name'] == name:
            return node
    return None

def get_progress():
    """Returns the combined progress dictionary for Flask API."""
    return {
        "upload": UPLOAD_PROGRESS["percent"], 
        "download": DOWNLOAD_PROGRESS["percent"],
        "status": UPLOAD_PROGRESS["status"] if UPLOAD_PROGRESS["status"] != "idle" else DOWNLOAD_PROGRESS["status"]
    }
 
# ------------------------------------------------------------
# Upload Logic
# ------------------------------------------------------------
def upload_to_namenode(filepath):
    """Sends entire file to the Namenode (which handles chunking and replication)."""
    UPLOAD_PROGRESS.update({"percent": 0.0, "status": "uploading"})
    try:
        filename = os.path.basename(filepath)
        filesize = os.path.getsize(filepath)

        s = connect_to_node(NAMENODE['host'], NAMENODE['port'])

        # Step 1: Announce upload (Protocol: UPLOAD:filename:filesize)
        s.send(f"UPLOAD:{filename}:{filesize}".encode())

        # Step 2: Wait for READY
        ack = s.recv(1024).decode()
        if ack != "READY":
            raise Exception(f"Namenode not ready → {ack}")

        # Step 3: Send file data
        sent = 0
        with open(filepath, "rb") as f:
            # We send the entire file to the NameNode, which does the chunking
            while chunk := f.read(4096): 
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
            raise Exception(f"Upload failed: {response}")

        s.close()

    except Exception as e:
        print(f"[ERROR] Upload failed: {e}")
        UPLOAD_PROGRESS["status"] = "failed"
    finally:
        if UPLOAD_PROGRESS["status"] != "failed":
             UPLOAD_PROGRESS.update({"percent": 100.0, "status": "done"})


# ------------------------------------------------------------
# Download Logic
# ------------------------------------------------------------
def metadata_request(filename):
    """Ask Namenode API for metadata."""
    namenode_api_url = f'http://{NAMENODE["host"]}:{NAMENODE["port"] + 2}/file_metadata'
    try:
        response = requests.get(namenode_api_url, timeout=5)
        response.raise_for_status()
        metadata = response.json()
        
        # We need the full file entry, not the entire metadata dict
        if filename in metadata:
            return metadata[filename]
        return None
        
    except Exception as e:
        print(f"[CLIENT ERROR] Metadata request failed → {e}")
        return None


def fetch_chunk_from_datanode(dn_info, filename, chunk_id, expected_checksum):
    """
    Fetches one chunk using the synchronized protocol (SIZE header + READY ack).
    Performs data integrity check.
    """
    try:
        s = connect_to_node(dn_info['host'], dn_info['port'])
        
        # Request (Protocol: RETRIEVE:filename:chunk_id)
        s.send(f"RETRIEVE:{filename}:{chunk_id}\n".encode())
        
        # Step 1: Read the SIZE header (e.g., SIZE:2097152\n)
        header = b""
        while not header.endswith(b"\n"):
            part = s.recv(1)
            if not part: break
            header += part
        
        header_str = header.decode().strip()
        if not header_str.startswith("SIZE:"):
            s.close()
            # The DataNode might send NOT_FOUND
            if header_str == "NOT_FOUND":
                return None
            raise Exception(f"Invalid header from DN: {header_str}")

        size = int(header_str.split(':')[1])
        s.send(b"READY") # Step 2: Acknowledge size with READY
        
        # Step 3: Receive the chunk data
        data = b""
        bytes_recd = 0
        while bytes_recd < size:
            chunk = s.recv(min(4096, size - bytes_recd))
            if not chunk: break
            data += chunk
            bytes_recd += len(chunk)
            
        s.close()
        
        if bytes_recd != size:
            raise Exception(f"Received incomplete chunk. Expected {size}, got {bytes_recd}")

        # Step 4: Data Integrity Check (CRUCIAL)
        actual_checksum = calculate_checksum(data)
        if actual_checksum != expected_checksum:
            raise Exception(f"Checksum failed! Expected {expected_checksum[:6]}..., got {actual_checksum[:6]}...")
        
        return data

    except Exception as e:
        print(f"[WARN] Failed to retrieve or verify chunk {chunk_id} from {dn_info['name']}: {e}")
        return None


def download_file(filename, output_path):
    """Requests chunks from Datanodes and rebuilds file."""
    DOWNLOAD_PROGRESS.update({"percent": 0.0, "status": "downloading"})
    
    # 1. Get metadata from NameNode
    file_metadata = metadata_request(filename)
    if not file_metadata:
        DOWNLOAD_PROGRESS.update({"percent": 0.0, "status": "failed"})
        print("[CLIENT] Metadata not found for download.")
        return

    total_chunks = file_metadata['num_chunks']
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "wb") as f:
        for i in range(total_chunks):
            chunk_entry = file_metadata['chunks'][i]
            chunk_data = None
            
            # The chunk entry MUST have a 'checksum' field now (NameNode needs to be updated)
            # For now, we assume a placeholder checksum if not implemented in NameNode
            expected_checksum = chunk_entry.get('checksum', '0' * 64) 

            # 2. Iterate through replicas (failover logic)
            # Use the NameNode's list of replica locations
            for replica in chunk_entry['replicas']: 
                
                # Fetch the full Datanode configuration (host/port)
                dn_info = get_datanode_by_name(replica['datanode'])
                if dn_info is None:
                    continue # Skip if config is missing

                chunk_data = fetch_chunk_from_datanode(dn_info, filename, i, expected_checksum)
                
                if chunk_data:
                    break # Success! Break out of the replica loop

            # 3. Write chunk and update progress
            if chunk_data:
                f.write(chunk_data)
                DOWNLOAD_PROGRESS["percent"] = round(((i + 1) / total_chunks) * 100, 2)
            else:
                # If all replicas fail, we cannot reconstruct the file.
                DOWNLOAD_PROGRESS.update({"percent": DOWNLOAD_PROGRESS["percent"], "status": "failed"})
                print(f"[CRITICAL] All replicas failed for chunk {i}. Download aborted.")
                return 

    print("[CLIENT] Download complete ✅")
    DOWNLOAD_PROGRESS.update({"percent": 100.0, "status": "done"})