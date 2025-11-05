from flask import Flask, render_template, request, jsonify
import requests
import json
from threading import Thread
import time
import client
import os

app = Flask(__name__)

progress = {"upload": 0, "download": 0, "status": "idle", "heartbeat": []}

# Load config
CONFIG = json.load(open('./config.json'))
NAMENODE_IP = CONFIG['namenode']['host']
NAMENODE_PORT = CONFIG['namenode']['port']


# def upload_worker(filepath):
#     progress["status"] = "uploading"
#     client.upload_file(filepath, lambda p: progress.update({"upload": p}))
#     progress["status"] = "done"

def download_worker(filename):
    progress["status"] = "downloading"
    client.download_file(filename, f"downloads/{filename}", lambda p: progress.update({"download": p}))
    progress["status"] = "done"

@app.route('/status')
def status():
    """
    Fetch health info from the NameNode heartbeat monitor endpoint.
    """
    try:
        # The namenode should have an endpoint that returns current status
        response = requests.get(f'http://{NAMENODE_IP}:{NAMENODE_PORT + 2}/heartbeat_status', timeout=3)
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            return jsonify({"error": "Failed to get status from NameNode"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# def download_worker(filename):
#     progress["status"] = "downloading"
#     for i in range(0, 100, 10):
#         progress["download"] = i
#         progress["heartbeat"] = [f"Datanode {j+1}: Alive ✅" for j in range(len(client.CONFIG['datanodes']))]
#         time.sleep(0.5)
#     progress["download"] = 100
#     progress["status"] = "done"

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload_file():
    uploaded_file = request.files.get("file")
    if not uploaded_file:
        return jsonify({"error": "No file provided"}), 400

    filepath = os.path.join("/tmp", uploaded_file.filename)
    uploaded_file.save(filepath)

    t = Thread(target=upload_worker, args=(filepath,))
    t.start()
    return jsonify({"message": f"Upload started for {uploaded_file.filename}"})


def upload_worker(filepath):
    try:
        client.upload_to_namenode(filepath)
    except Exception as e:
        print(f"[ERROR] Upload failed: {e}")


@app.route("/download", methods=["POST"])
def download_file():
    filename = request.form.get("filename")
    t = Thread(target=download_worker, args=(filename,))
    t.start()
    return jsonify({"message": "Download started"})

@app.route('/progress')
def progress():
    return jsonify(client.get_progress())

# @app.route("/progress")
# def get_progress():
#     return jsonify(progress)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)
