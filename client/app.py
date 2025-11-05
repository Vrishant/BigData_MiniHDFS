# client/app.py
from flask import Flask, render_template, request, jsonify
import requests
import json
from threading import Thread
import os
import client # The corrected client logic

app = Flask(__name__)

# Load config
CONFIG = json.load(open('./config.json'))
NAMENODE_IP = CONFIG['namenode']['host']
NAMENODE_PORT = CONFIG['namenode']['port']

# Ensure 'downloads' directory exists
os.makedirs("downloads", exist_ok=True)


def upload_worker(filepath):
    """Worker thread that calls the core client logic for upload."""
    client.upload_to_namenode(filepath)
    # The client module handles updating its own progress state


def download_worker(filename):
    """Worker thread that calls the core client logic for download."""
    output_path = os.path.join("downloads", filename)
    client.download_file(filename, output_path)
    # The client module handles updating its own progress state

@app.route('/status')
def status():
    """
    Fetch health info from the NameNode heartbeat monitor endpoint.
    """
    # The Flask API runs on NAMENODE_PORT + 2
    namenode_api_url = f'http://{NAMENODE_IP}:{NAMENODE_PORT + 2}/heartbeat_status'
    try:
        response = requests.get(namenode_api_url, timeout=3)
        response.raise_for_status() # Raise exception for 4xx or 5xx errors
        return jsonify(response.json())
    except Exception as e:
        return jsonify({"error": f"Failed to get status from NameNode API: {e}"}), 500

@app.route("/")
def home():
    """Renders the main dashboard page."""
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload_file():
    """Handles file receipt from the form and starts upload thread."""
    uploaded_file = request.files.get("file")
    if not uploaded_file:
        return jsonify({"error": "No file provided"}), 400

    filepath = os.path.join("/tmp", uploaded_file.filename)
    uploaded_file.save(filepath)

    t = Thread(target=upload_worker, args=(filepath,))
    t.start()
    return jsonify({"message": f"Upload started for {uploaded_file.filename}"})

@app.route("/download", methods=["POST"])
def download_file():
    """Starts the file download thread."""
    filename = request.form.get("filename")
    if not filename:
        return jsonify({"error": "No filename provided"}), 400
        
    t = Thread(target=download_worker, args=(filename,))
    t.start()
    return jsonify({"message": f"Download started for {filename}"})

@app.route('/progress')
def get_progress():
    """Fetches and returns the current upload/download progress from client.py."""
    return jsonify(client.get_progress())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)