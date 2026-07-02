import os, re, time, json, random, hashlib, logging, threading, shutil, struct
from pathlib import Path
from functools import wraps
from urllib.parse import unquote
from collections import defaultdict

import yt_dlp
from flask import Flask, Response, render_template, jsonify, request, send_file, abort, send_from_directory
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
MUSIC_DIR = BASE_DIR / "Music"
DOWNLOADS_DIR = BASE_DIR / "Downloads"
STATIC_DIR = BASE_DIR / "static"
META_DIR = BASE_DIR / ".meta"
MAX_FILE_SIZE = 200 * 1024 * 1024
ALLOWED_EXTENSIONS = {".mp3", ".wav", ".ogg", ".flac", ".m4a", ".aac", ".wma"}
SECRET_KEY = os.getenv("SECRET_KEY", hashlib.sha256(os.urandom(32)).hexdigest())
HOST = os.getenv("RADIO_HOST", "127.0.0.1")
PORT = int(os.getenv("RADIO_PORT", "8080"))
BASIC_AUTH_USERNAME = os.getenv("RADIO_USERNAME", "")
BASIC_AUTH_PASSWORD = os.getenv("RADIO_PASSWORD", "")
MAX_DOWNLOADS_PER_IP = int(os.getenv("MAX_DOWNLOADS_PER_IP", "10"))

for d in (MUSIC_DIR, DOWNLOADS_DIR, META_DIR):
    d.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("radio")

app = Flask(__name__)
app.secret_key = SECRET_KEY

# ── Rate limiter ───────────────────────────────────────────────────────
_rate_buckets: dict[str, list[float]] = {}
_RATE_LIMIT, _RATE_WINDOW = 60, 60

def rate_limit(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        ip = request.remote_addr or "unknown"
        now = time.time()
        bucket = _rate_buckets.setdefault(ip, [])
        bucket[:] = [t for t in bucket if now - t < _RATE_WINDOW]
        if len(bucket) >= _RATE_LIMIT:
            abort(429)
        bucket.append(now)
        return f(*args, **kwargs)
    return wrapper

# ── Security headers ───────────────────────────────────────────────────
_SECURITY_HEADERS = {"X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY",
                     "X-XSS-Protection": "0", "Referrer-Policy": "strict-origin-when-cross-origin"}

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
            return Response("Unauthorized", 401, {"WWW-Authenticate": 'Basic realm="Radio IsraProyets"'})
        return f(*args, **kwargs)
    return wrapper

def safe_song_path(filename: str) -> Path | None:
    name = secure_filename(Path(unquote(filename)).name)
    if not name:
        return None
    path = (MUSIC_DIR / name).resolve()
    if not str(path).startswith(str(MUSIC_DIR.resolve())) or not path.is_file():
        return None
    if path.suffix.lower() not in ALLOWED_EXTENSIONS or path.stat().st_size > MAX_FILE_SIZE:
        return None
    return path

@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory(STATIC_DIR, filename)

# ── Metadata helpers ───────────────────────────────────────────────────
_META_FILE = META_DIR / "yt_sources.json"

def _load_yt_sources() -> set[str]:
    try:
        return set(json.loads(_META_FILE.read_text()))
    except Exception:
        return set()

def _save_yt_sources(sources: set[str]):
    _META_FILE.write_text(json.dumps(list(sources), indent=2))

def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

# ── Music scanning ─────────────────────────────────────────────────────
def scan_music() -> list[Path]:
    files = []
    for ext in ALLOWED_EXTENSIONS:
        files.extend(MUSIC_DIR.rglob(f"*{ext}"))
    return sorted(files)

def song_metadata(path: Path) -> dict:
    yt_sources = _load_yt_sources()
    fname = path.name
    return {
        "id": hashlib.md5(str(path).encode()).hexdigest()[:12],
        "name": path.stem,
        "filename": fname,
        "size": path.stat().st_size,
        "ext": path.suffix.lower(),
        "sha256": _sha256_file(path),
        "from_yt": fname in yt_sources,
    }

# ── Duplicate detection ────────────────────────────────────────────────
def _normalize(s: str) -> str:
    s = re.sub(r'[\(\[].*?[\)\]]', '', s)
    s = re.sub(r'[^a-z0-9]', '', s.lower())
    return s.strip()

def find_duplicates() -> list[dict]:
    songs = scan_music()
    meta = [song_metadata(s) for s in songs]
    by_sha: dict[str, list] = defaultdict(list)
    by_name: dict[str, list] = defaultdict(list)
    for m in meta:
        by_sha[m["sha256"]].append(m)
        by_name[_normalize(m["name"])].append(m)

    seen = set()
    groups = []
    for m in meta:
        if m["id"] in seen:
            continue
        group = {}
        group["sha_matches"] = [x for x in by_sha[m["sha256"]] if x["id"] != m["id"]]
        group["name_matches"] = [x for x in by_name[_normalize(m["name"])] if x["id"] != m["id"] and x["id"] not in {y["id"] for y in group["sha_matches"]}]
        if group["sha_matches"] or group["name_matches"]:
            group["original"] = m
            groups.append(group)
            seen.add(m["id"])
            for x in group["sha_matches"] + group["name_matches"]:
                seen.add(x["id"])
    return groups

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
            "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}],
            "progress_hooks": [progress_hook], "quiet": True, "no_warnings": True, "extract_flat": False,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            title = info.get("title", "unknown")
        yt_sources = _load_yt_sources()
        for f in out_dir.iterdir():
            if f.suffix.lower() == ".mp3":
                dest = MUSIC_DIR / f.name
                counter = 1
                while dest.exists():
                    dest = MUSIC_DIR / f"{f.stem}_{counter}.mp3"
                    counter += 1
                f.rename(dest)
                yt_sources.add(dest.name)
        _save_yt_sources(yt_sources)
        shutil.rmtree(out_dir, ignore_errors=True)
        with _download_lock:
            if task_id in _download_tasks:
                _download_tasks[task_id].update({"status": "done", "title": title})
    except Exception as e:
        log.error("Download failed: %s", e)
        with _download_lock:
            if task_id in _download_tasks:
                _download_tasks[task_id].update({"status": "error", "error": str(e)})

# ── Routes ─────────────────────────────────────────────────────────────
@app.route("/")
@require_auth
@rate_limit
def index():
    return render_template("index.html")

@app.route("/stream/<song_id>")
@require_auth
@rate_limit
def stream_song(song_id):
    for s in scan_music():
        if hashlib.md5(str(s).encode()).hexdigest()[:12] == song_id:
            return send_file(s, mimetype="audio/mpeg", as_attachment=False, conditional=True)
    abort(404)

@app.route("/stream")
@require_auth
@rate_limit
def stream():
    songs = scan_music()
    if not songs:
        return Response("", 404)
    def generate():
        while True:
            random.shuffle(songs)
            for song in songs:
                try:
                    with open(song, "rb") as f:
                        while chunk := f.read(262144):
                            yield chunk
                except Exception:
                    continue
    return Response(generate(), mimetype="audio/mpeg")

@app.route("/api/songs")
@rate_limit
def list_songs():
    return jsonify([song_metadata(s) for s in scan_music()])

@app.route("/api/search")
@rate_limit
def search_songs():
    q = request.args.get("q", "").lower()
    if not q:
        return jsonify([])
    return jsonify([song_metadata(s) for s in scan_music() if q in s.stem.lower()])

@app.route("/api/download", methods=["POST"])
@require_auth
@rate_limit
def download_song():
    data = request.get_json(silent=True)
    if not data or "url" not in data:
        return jsonify({"error": "Missing URL"}), 400
    url = data["url"].strip()
    if not url.startswith(("http://", "https://")) or re.search(r"[;&|`$()\[\]{}]", url):
        return jsonify({"error": "Invalid URL"}), 400
    ip = request.remote_addr or "unknown"
    task_id = hashlib.sha256(f"{url}{time.time()}{ip}".encode()).hexdigest()[:16]
    with _download_lock:
        ip_count = sum(1 for t in _download_tasks.values() if t.get("ip") == ip and t["status"] in ("downloading", "converting"))
        if ip_count >= MAX_DOWNLOADS_PER_IP:
            return jsonify({"error": "Too many active downloads"}), 429
        _download_tasks[task_id] = {"id": task_id, "url": url, "status": "queued", "progress": 0.0, "speed": 0, "eta": 0, "title": None, "error": None, "ip": ip}
    threading.Thread(target=_run_download, args=(task_id, url), daemon=True).start()
    return jsonify({"task_id": task_id}), 202

@app.route("/api/download/<task_id>")
@rate_limit
def download_status(task_id):
    with _download_lock:
        task = _download_tasks.get(task_id)
    if not task:
        return jsonify({"error": "Not found"}), 404
    return jsonify({k: v for k, v in task.items() if k != "ip"})

@app.route("/api/download/<task_id>", methods=["DELETE"])
@require_auth
@rate_limit
def cancel_download(task_id):
    with _download_lock:
        _download_tasks.pop(task_id, None)
    return jsonify({"ok": True})

@app.route("/api/duplicates")
@require_auth
@rate_limit
def get_duplicates():
    return jsonify(find_duplicates())

@app.route("/api/duplicates/clean", methods=["POST"])
@require_auth
@rate_limit
def clean_duplicates():
    groups = find_duplicates()
    removed = []
    for g in groups:
        dupes = g["sha_matches"] + g["name_matches"]
        for d in dupes:
            path = MUSIC_DIR / d["filename"]
            if path.exists():
                path.unlink()
                removed.append(d["filename"])
    yt_sources = _load_yt_sources()
    yt_sources -= set(removed)
    _save_yt_sources(yt_sources)
    return jsonify({"removed": len(removed), "files": removed})

@app.route("/api/duplicates/<song_id>", methods=["DELETE"])
@require_auth
@rate_limit
def delete_song(song_id):
    for s in scan_music():
        if hashlib.md5(str(s).encode()).hexdigest()[:12] == song_id:
            s.unlink()
            yt_sources = _load_yt_sources()
            yt_sources.discard(s.name)
            _save_yt_sources(yt_sources)
            return jsonify({"ok": True})
    abort(404)

@app.route("/api/health")
def health():
    songs = scan_music()
    return jsonify({"status": "ok", "songs": len(songs),
        "downloading": sum(1 for t in _download_tasks.values() if t["status"] in ("downloading", "converting", "queued"))})

if __name__ == "__main__":
    log.info("─" * 40)
    log.info("  Radio IsraProyets  |  http://%s:%d", HOST, PORT)
    log.info("  Auth: %s", "enabled" if BASIC_AUTH_USERNAME else "disabled")
    log.info("─" * 40)
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
