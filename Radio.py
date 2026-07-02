import os
import re
import time
import json
import uuid
import random
import hashlib
import logging
import threading
import subprocess
from pathlib import Path
from functools import wraps
from urllib.parse import unquote

import yt_dlp
from flask import (
    Flask, Response, render_template, jsonify, request,
    send_file, abort, redirect, url_for, session
)
from werkzeug.utils import secure_filename

# ── Config ─────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
MUSIC_DIR = BASE_DIR / "Music"
DOWNLOADS_DIR = BASE_DIR / "Downloads"
MAX_FILE_SIZE = 200 * 1024 * 1024  # 200 MB
ALLOWED_EXTENSIONS = {".mp3", ".wav", ".ogg", ".flac", ".m4a", ".aac", ".wma"}
SECRET_KEY = os.getenv("SECRET_KEY", hashlib.sha256(os.urandom(32)).hexdigest())
HOST = os.getenv("RADIO_HOST", "127.0.0.1")
PORT = int(os.getenv("RADIO_PORT", "8080"))
BASIC_AUTH_USERNAME = os.getenv("RADIO_USERNAME", "")
BASIC_AUTH_PASSWORD = os.getenv("RADIO_PASSWORD", "")
MAX_DOWNLOADS_PER_IP = int(os.getenv("MAX_DOWNLOADS_PER_IP", "10"))

MUSIC_DIR.mkdir(exist_ok=True)
DOWNLOADS_DIR.mkdir(exist_ok=True)

# ── Logging ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("radio")

# ── App ────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = SECRET_KEY

# ── Rate limiter (simple token bucket per IP) ──────────────────────────
_rate_buckets: dict[str, list[float]] = {}
_RATE_LIMIT = 60  # requests
_RATE_WINDOW = 60  # seconds


def rate_limit(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        ip = request.remote_addr or "unknown"
        now = time.time()
        bucket = _rate_buckets.setdefault(ip, [])
        bucket[:] = [t for t in bucket if now - t < _RATE_WINDOW]
        if len(bucket) >= _RATE_LIMIT:
            abort(429, description="Too many requests")
        bucket.append(now)
        return f(*args, **kwargs)
    return wrapper


# ── Security helpers ───────────────────────────────────────────────────
_SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "X-XSS-Protection": "0",
    "Referrer-Policy": "strict-origin-when-cross-origin",
}


@app.after_request
def add_security_headers(resp):
    for k, v in _SECURITY_HEADERS.items():
        resp.headers.setdefault(k, v)
    resp.headers.setdefault("Server", "Radio-IsraelProyets")
    return resp


def require_auth(f):
    if not BASIC_AUTH_USERNAME:
        return f

    @wraps(f)
    def wrapper(*args, **kwargs):
        auth = request.authorization
        if not auth or auth.username != BASIC_AUTH_USERNAME or auth.password != BASIC_AUTH_PASSWORD:
            return Response(
                "Unauthorized", 401,
                {"WWW-Authenticate": 'Basic realm="Radio IsraProyets"'},
            )
        return f(*args, **kwargs)
    return wrapper


def safe_song_path(filename: str) -> Path | None:
    name = Path(unquote(filename)).name
    name = secure_filename(name)
    if not name:
        return None
    path = MUSIC_DIR / name
    path = path.resolve()
    if not str(path).startswith(str(MUSIC_DIR.resolve())):
        return None
    if not path.exists() or not path.is_file():
        return None
    if path.suffix.lower() not in ALLOWED_EXTENSIONS:
        return None
    if path.stat().st_size > MAX_FILE_SIZE:
        return None
    return path


# ── Music scanning ─────────────────────────────────────────────────────
def scan_music() -> list[Path]:
    files = []
    for ext in ALLOWED_EXTENSIONS:
        files.extend(MUSIC_DIR.rglob(f"*{ext}"))
    return sorted(files)


def song_metadata(path: Path) -> dict:
    return {
        "id": hashlib.md5(str(path).encode()).hexdigest()[:12],
        "name": path.stem,
        "filename": path.name,
        "size": path.stat().st_size,
        "ext": path.suffix.lower(),
    }


# ── yt-dlp download ────────────────────────────────────────────────────
_download_tasks: dict[str, dict] = {}
_download_lock = threading.Lock()


def _run_download(task_id: str, url: str) -> None:
    def progress_hook(d):
        with _download_lock:
            task = _download_tasks.get(task_id)
            if not task:
                return
            status = d.get("status", "")
            if status == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
                downloaded = d.get("downloaded_bytes", 0)
                task["progress"] = downloaded / total if total > 0 else 0
                task["speed"] = d.get("speed", 0)
                task["eta"] = d.get("eta", 0)
            elif status == "finished":
                task["progress"] = 1.0
                task["status"] = "converting"

    try:
        out_dir = DOWNLOADS_DIR / task_id
        out_dir.mkdir(exist_ok=True)

        ydl_opts = {
            "format": "bestaudio/best",
            "outtmpl": str(out_dir / "%(title)s.%(ext)s"),
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
            "progress_hooks": [progress_hook],
            "quiet": True,
            "no_warnings": True,
            "extract_flat": False,
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            title = info.get("title", "unknown")

        # Move the resulting mp3 to Music dir
        for f in out_dir.iterdir():
            if f.suffix.lower() == ".mp3":
                dest = MUSIC_DIR / f.name
                # Avoid name collisions
                counter = 1
                while dest.exists():
                    stem = f.stem + f"_{counter}"
                    dest = MUSIC_DIR / f"{stem}.mp3"
                    counter += 1
                f.rename(dest)

        # Cleanup
        import shutil
        shutil.rmtree(out_dir, ignore_errors=True)

        with _download_lock:
            if task_id in _download_tasks:
                _download_tasks[task_id]["status"] = "done"
                _download_tasks[task_id]["title"] = title

    except Exception as e:
        log.error("Download failed: %s", e)
        with _download_lock:
            if task_id in _download_tasks:
                _download_tasks[task_id]["status"] = "error"
                _download_tasks[task_id]["error"] = str(e)


# ── Routes ─────────────────────────────────────────────────────────────
@app.route("/")
@require_auth
@rate_limit
def index():
    songs = scan_music()
    return render_template("index.html", songs=[song_metadata(s) for s in songs])


@app.route("/stream/<song_id>")
@require_auth
@rate_limit
def stream_song(song_id):
    songs = scan_music()
    target = None
    for s in songs:
        if hashlib.md5(str(s).encode()).hexdigest()[:12] == song_id:
            target = s
            break
    if not target:
        abort(404)

    return send_file(
        target,
        mimetype="audio/mpeg",
        as_attachment=False,
        conditional=True,
    )


@app.route("/stream")
@require_auth
@rate_limit
def stream():
    """Radio stream — shuffle all songs."""
    songs = scan_music()
    if not songs:
        return Response("No songs available", status=404, mimetype="text/plain")

    def generate():
        while True:
            random.shuffle(songs)
            for song in songs:
                try:
                    chunk_size = 256 * 1024
                    with open(song, "rb") as f:
                        while True:
                            chunk = f.read(chunk_size)
                            if not chunk:
                                break
                            yield chunk
                except Exception:
                    continue

    return Response(generate(), mimetype="audio/mpeg")


@app.route("/api/now-playing")
@rate_limit
def now_playing():
    songs = scan_music()
    if not songs:
        return jsonify({"name": "No songs available"})
    return jsonify({"name": random.choice(songs).stem})


@app.route("/api/songs")
@rate_limit
def list_songs():
    songs = scan_music()
    return jsonify([song_metadata(s) for s in songs])


@app.route("/api/search")
@rate_limit
def search_songs():
    q = request.args.get("q", "").lower()
    if not q:
        return jsonify([])
    songs = scan_music()
    results = [song_metadata(s) for s in songs if q in s.stem.lower()]
    return jsonify(results)


# ── Download routes ────────────────────────────────────────────────────
@app.route("/api/download", methods=["POST"])
@require_auth
@rate_limit
def download_song():
    data = request.get_json(silent=True)
    if not data or "url" not in data:
        return jsonify({"error": "Missing URL"}), 400

    url = data["url"].strip()
    if not url.startswith(("http://", "https://")):
        return jsonify({"error": "Invalid URL"}), 400

    # Block dangerous patterns
    blocked = re.search(r"[;&|`$()\[\]{}]", url)
    if blocked:
        return jsonify({"error": "Invalid URL"}), 400

    ip = request.remote_addr or "unknown"
    task_id = hashlib.sha256(f"{url}{time.time()}{ip}".encode()).hexdigest()[:16]

    with _download_lock:
        # Count active downloads for this IP
        ip_count = sum(
            1 for t in _download_tasks.values()
            if t.get("ip") == ip and t["status"] in ("downloading", "converting")
        )
        if ip_count >= MAX_DOWNLOADS_PER_IP:
            return jsonify({"error": "Too many active downloads"}), 429

        _download_tasks[task_id] = {
            "id": task_id,
            "url": url,
            "status": "queued",
            "progress": 0.0,
            "speed": 0,
            "eta": 0,
            "title": None,
            "error": None,
            "ip": ip,
        }

    thread = threading.Thread(target=_run_download, args=(task_id, url), daemon=True)
    thread.start()

    return jsonify({"task_id": task_id}), 202


@app.route("/api/download/<task_id>")
@rate_limit
def download_status(task_id):
    with _download_lock:
        task = _download_tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify({k: v for k, v in task.items() if k != "ip"})


@app.route("/api/download/<task_id>", methods=["DELETE"])
@require_auth
@rate_limit
def cancel_download(task_id):
    with _download_lock:
        if task_id in _download_tasks:
            del _download_tasks[task_id]
    return jsonify({"ok": True})


# ── Health ─────────────────────────────────────────────────────────────
@app.route("/api/health")
def health():
    songs = scan_music()
    return jsonify({
        "status": "ok",
        "songs": len(songs),
        "downloading": sum(
            1 for t in _download_tasks.values()
            if t["status"] in ("downloading", "converting", "queued")
        ),
    })


# ── Main ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mode = "production" if HOST != "127.0.0.1" else "development (localhost)"
    log.info("─" * 40)
    log.info("  Radio IsraProyets")
    log.info("  Mode: %s", mode)
    log.info("  URL:  http://%s:%d/", HOST, PORT)
    log.info("  Auth: %s", "enabled" if BASIC_AUTH_USERNAME else "disabled")
    log.info("─" * 40)
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
