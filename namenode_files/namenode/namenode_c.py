# namenode/namenode_c.py
import socket
import threading
import json
import time
import os
import hashlib

# --- IMPORT SHARED STATE & CONFIG ---
# Import all required variables and the crucial lock from shared_state
from shared_state import (
    CONFIG, METADATA_LOCK, metadata, 
    datanode_heartbeat, DATANODES, INACTIVE_NODES
)

NAMENODE_HOST = CONFIG['namenode']['host']
NAMENODE_PORT = CONFIG['namenode']['port']
REPL_FACTOR = CONFIG['replication_factor']
CHUNK_SIZE = 2 * 1024 * 1024
HEARTBEAT_TIMEOUT = 20 # seconds

# ------------------------------
# Metadata Management
# ------------------------------
def save_metadata():
    """Saves metadata to disk. Assumes lock is already held by caller."""
    try:
        with open(CONFIG['metadata_file'], "w") as f:
            json.dump(metadata, f, indent=4)
    except Exception as e:
        print(f"[ERROR] Failed to save metadata: {e}")

# ------------------------------
# Datanode Communication Helpers
# ------------------------------
def get_datanode_info(name):
    """Utility to get host/port from Datanode name."""
    for dn in DATANODES:
        if dn['name'] == name:
            return dn
    return None

def send_chunk_to_datanode(dn_info, filename, chunk_id, chunk_data):
    """Sends a chunk to a specific DataNode for storage."""
    # [Original implementation remains the same for simplicity]
    try:
        s = socket.socket()
        s.settimeout(10)
        s.connect((dn_info['host'], dn_info['port']))

        header = f"STORE:{filename}:{chunk_id}:{len(chunk_data)}\n".encode()
        s.sendall(header)

        # Wait for Datanode to be ready
        ack = s.recv(16)
        if ack != b"READY":
            s.close()
            return False

        # Send the data and end marker
        s.sendall(chunk_data) 
        s.sendall(b"<END>") 
        
        response = s.recv(32)
        s.close()
        return response == b"STORED"
    except Exception as e:
        print(f"[ERROR] Sending chunk {chunk_id} to {dn_info['name']} failed: {e}")
        return False

def fetch_chunk_from_datanode(dn_info, filename, chunk_id):
    """
    Requests a chunk from a Datanode. 
    Crucial for re-replication and client downloads.
    """
    try:
        s = socket.socket()
        s.settimeout(10)
        s.connect((dn_info['host'], dn_info['port']))

        # Send retrieval request (Datanode must handle this)
        request = f"RETRIEVE:{filename}:{chunk_id}\n".encode()
        s.sendall(request)
        
        # Expecting a header: "SIZE:{length}\n"
        header = s.recv(1024).decode()
        if not header.startswith("SIZE:"):
            s.close()
            return None 

        size = int(header.split(':')[1])
        s.send(b"READY") # Acknowledge receipt of size
        
        # Receive the chunk data
        data = b""
        bytes_recd = 0
        while bytes_recd < size:
            chunk = s.recv(min(4096, size - bytes_recd))
            if not chunk: break
            data += chunk
            bytes_recd += len(chunk)
            
        s.close()
        if bytes_recd == size:
            return data
        else:
            print(f"[WARN] Received {bytes_recd} but expected {size} from {dn_info['name']}")
            return None
            
    except Exception as e:
        print(f"[ERROR] Fetching chunk {chunk_id} from {dn_info['name']} failed: {e}")
        return None

# ------------------------------
# Core Fault Tolerance: Re-Replication Logic
# ------------------------------

def calculate_checksum(data):
    """Calculate SHA-256 hash for data integrity."""
    return hashlib.sha256(data).hexdigest()

def find_under_replicated_chunks(dead_node_name):
    """
    Searches metadata for chunks that now have fewer replicas than REPL_FACTOR.
    Must be called while holding the lock.
    """
    chunks_to_repair = []
    
    # Iterate through all files and their chunks
    for filename, file_data in metadata.items():
        for chunk_entry in file_data['chunks']:
            
            replicas = chunk_entry['replicas']
            
            # Count how many live replicas exist (not on the dead node)
            live_replicas = [r for r in replicas if r['datanode'] != dead_node_name]
            
            if len(live_replicas) < REPL_FACTOR:
                # We need to replicate this chunk
                # Crucial check: make sure we have at least ONE live replica to copy from
                if live_replicas:
                    source_datanode = live_replicas[0]['datanode']
                    chunks_to_repair.append({
                        "filename": filename,
                        "chunk_id": chunk_entry['chunk_id'],
                        "source_node": source_datanode
                    })
                else:
                    print(f"[CRITICAL] Chunk {chunk_entry['chunk_id']} of {filename} is lost! No live replicas found.")
                    # In a real HDFS, this would be logged and flagged.
                    
    return chunks_to_repair

def replicate_chunk(filename, chunk_id, source_node_name):
    """
    Executes the re-replication of a single chunk.
    """
    source_dn_info = get_datanode_info(source_node_name)
    if not source_dn_info:
        print(f"[ERROR] Source Datanode {source_node_name} not found in config.")
        return False
    
    # 1. Choose a NEW Datanode for the replica (must not be the source or the dead one)
    # We choose the first available node that is not currently storing the chunk
    with METADATA_LOCK:
        # Get current locations (including the dead one for now)
        current_replicas = next(
            c['replicas'] for f, c in metadata.items() 
            if f == filename for c in c['chunks'] 
            if c['chunk_id'] == chunk_id
        )
        current_nodes = {r['datanode'] for r in current_replicas}
        
        # Select the first Datanode not currently holding a copy
        target_dn_info = next(
            (dn for dn in DATANODES if dn['name'] not in current_nodes and dn['name'] not in INACTIVE_NODES), 
            None
        )

    if not target_dn_info:
        print("[WARN] Could not find a suitable Datanode for re-replication.")
        return False
        
    print(f"[REPLICATING] Chunk {chunk_id} of {filename}. From {source_node_name} to {target_dn_info['name']}")

    # 2. Fetch the data from the source Datanode
    chunk_data = fetch_chunk_from_datanode(source_dn_info, filename, chunk_id)
    if not chunk_data:
        print(f"[ERROR] Failed to fetch chunk {chunk_id} from {source_node_name}.")
        return False

    # 3. Send the data to the target Datanode
    success = send_chunk_to_datanode(target_dn_info, filename, chunk_id, chunk_data)
    
    # 4. Update the metadata (CRITICAL)
    if success:
        with METADATA_LOCK:
            # Find the correct chunk entry and add the new replica location
            for file_data in metadata.values():
                for chunk_entry in file_data['chunks']:
                    if chunk_entry['chunk_id'] == chunk_id:
                        # Remove the dead node's location and add the new one
                        chunk_entry['replicas'] = [
                            r for r in chunk_entry['replicas'] 
                            if r['datanode'] not in INACTIVE_NODES
                        ]
                        chunk_entry['replicas'].append({
                            "datanode": target_dn_info["name"],
                            "host": target_dn_info["host"],
                            "port": target_dn_info["port"]
                        })
                        break
            save_metadata()
            print(f"[SUCCESS] Re-replication of chunk {chunk_id} complete.")
    
    return success

# ------------------------------
# Heartbeat
# ------------------------------
def heartbeat_listener():
    s = socket.socket()
    # Heartbeat listener runs on NAMENODE_PORT + 1 (e.g., 5001)
    s.bind((NAMENODE_HOST, NAMENODE_PORT + 1))
    s.listen(5)
    print(f"[NAMENODE] Heartbeat listener on port {NAMENODE_PORT + 1}")
    while True:
        conn, _ = s.accept()
        try:
            data = conn.recv(1024).decode()
            node_info = json.loads(data)
            print(f"[HEARTBEAT] Received from {node_info['node']}")

            with METADATA_LOCK: # Protect the shared heartbeat data
                datanode_heartbeat[node_info['node']] = time.time()
                
            # If a node was inactive and sends a heartbeat, mark it as recovered
            if node_info['node'] in INACTIVE_NODES:
                print(f"[RECOVERED] {node_info['node']} is back")
                with METADATA_LOCK:
                    INACTIVE_NODES.remove(node_info['node'])
                    # Future task: check if any chunks need moving off this recovered node
                    
        except Exception as e:
            print(f"[ERROR] Heartbeat listener: {e}")
        finally:
            conn.close()

def heartbeat_monitor():
    while True:
        time.sleep(5)
        now = time.time()
        
        nodes_to_check = list(DATANODES) 

        for node in nodes_to_check:
            node_name = node['name']
            
            with METADATA_LOCK:
                last_heartbeat = datanode_heartbeat.get(node_name, 0)
                is_currently_inactive = node_name in INACTIVE_NODES
            
            # --- The Key Logic Change ---
            # Condition 1: Check if the node has EVER checked in AND if its last check-in is too old.
            if last_heartbeat > 0 and (now - last_heartbeat > HEARTBEAT_TIMEOUT):
                if not is_currently_inactive:
                    print(f"\n[ALERT - DOWN] {node_name} is DOWN. Initiating repair.")
                    # ... (rest of the DOWN logic: INACTIVE_NODES.add, find_under_replicated_chunks, replicate_chunk) ...
            
            # Condition 2: If last_heartbeat is 0, we simply wait for the first real heartbeat.
            # We skip the "if not is_currently_inactive" check for this condition
            # because a node with last_heartbeat=0 can't be inactive yet.

            elif last_heartbeat <= 0:
                # Optional: Log a waiting message for clarity during startup
                print(f"[STARTUP] Waiting for initial heartbeat from {node_name}...")
                pass # Just wait, no alert needed.
                
            elif now - last_heartbeat <= HEARTBEAT_TIMEOUT and is_currently_inactive:
                # This is the node recovery part (already mostly handled in heartbeat_listener)
                pass
                
# ------------------------------
# Client Handler
# ------------------------------
# ... (handle_client and listen_for_client functions remain the same 
# but need the METADATA_LOCK in handle_client) ...


def handle_client(conn, addr):
    """
    Handles a single client connection for file upload.
    Responsible for receiving the file, chunking it, coordinating 
    replication with Datanodes, and updating metadata.
    """
    print(f"[CLIENT CONNECTED] {addr}")
    
    # Initialize variables to prevent UnboundLocalError in the except block 
    # if an error occurs during the very first steps.
    filename = "UNKNOWN_FILE" 
    size = 0
    file_data = b""

    try:
        # --- 1. Receive Initial Header and Validate Request ---
        data = conn.recv(1024).decode()
        if not data.startswith("UPLOAD"):
            conn.send(b"INVALID_REQUEST: Expected UPLOAD command.")
            return

        # Header format: UPLOAD:filename:size
        _, filename, size_str = data.split(":")
        size = int(size_str)
        print(f"[UPLOAD INIT] {filename} ({size} bytes)")

        # Acknowledge the header and tell the client to start sending data
        conn.send(b"READY")

        # --- 2. Receive Full File Data from Client ---
        file_data = b""
        total_recd = 0
        end_marker = b"<END>"
        
        # Note: The client is expected to send all data followed by the <END> marker.
        while True:
            # We use a smaller buffer size to check for the <END> marker frequently
            chunk = conn.recv(4096) 
            if not chunk:
                break

            if end_marker in chunk:
                file_data += chunk.replace(end_marker, b"")
                total_recd += len(chunk.replace(end_marker, b""))
                break
                
            file_data += chunk
            total_recd += len(chunk)

        # Basic integrity check before processing
        if len(file_data) != size:
            raise Exception(f"Received size mismatch: Expected {size} bytes, got {len(file_data)} bytes.")

        # --- 3. Chunking and Replication Coordination ---
        num_chunks = (size // CHUNK_SIZE) + (1 if size % CHUNK_SIZE else 0)
        
        metadata_entry = {
            "filesize": size,
            "num_chunks": num_chunks,
            "chunks": [] # Will hold metadata for each chunk
        }

        # Iterate through the file data, splitting it into 2MB chunks
        for i in range(num_chunks):
            chunk_data = file_data[i * CHUNK_SIZE:(i + 1) * CHUNK_SIZE]
            
            # --- CRITICAL FIX: Calculate Checksum ---
            chunk_checksum = calculate_checksum(chunk_data) 

            # Simple Round-Robin for initial assignment to Datanodes
            # Ensures chunks (i) and (i+1) don't go to the exact same nodes first
            assigned_nodes = [DATANODES[(i + j) % len(DATANODES)] for j in range(REPL_FACTOR)]
            replicas = []

            for node in assigned_nodes:
                # Send the chunk data and wait for confirmation
                success = send_chunk_to_datanode(node, filename, i, chunk_data)
                
                if success:
                    replicas.append({
                        "datanode": node["name"],
                        "host": node["host"],
                        "port": node["port"]
                    })
                else:
                    # In a real system, we would immediately try another node, 
                    # but here we simply skip the failed replica and log a warning.
                    print(f"[WARN] Failed to store chunk {i} on {node['name']}. Under-replicated!")

            # metadata_entry["chunks"].append({
            #     "chunk_id": i,
            #     "replicas": replicas # List of Datanodes that successfully stored the chunk
            # })
            metadata_entry["chunks"].append({
                "chunk_id": i,
                "replicas": replicas, 
                "checksum": chunk_checksum # <--- ADD THIS TO METADATA
            })

        # --- 4. Finalize Metadata and Acknowledge Client ---
        
        # Use the lock to ensure no other thread reads/writes metadata 
        # while we are making this fundamental change.
        with METADATA_LOCK:
            metadata[filename] = metadata_entry
            save_metadata()
            
        conn.send(b"UPLOAD_SUCCESS")
        print(f"[UPLOAD COMPLETE] {filename} uploaded successfully.")

    except Exception as e:
        # If an error happens, log it and inform the client.
        print(f"[ERROR] Client handler failed for {filename} ({addr}): {e}")
        # Send an error back. We only use variables guaranteed to be defined (like e).
        conn.send(f"ERROR: Upload failed for {filename}. Details: {e}".encode())
        
    finally:
        # Always close the connection to free up resources
        conn.close()


def listen_for_client():
    s = socket.socket()
    s.bind((NAMENODE_HOST, NAMENODE_PORT))
    s.listen(5)
    print(f"[NAMENODE] Listening for clients on {NAMENODE_HOST}:{NAMENODE_PORT}")
    while True:
        conn, addr = s.accept()
        # Start a new thread for each client connection
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()