import os
import random
import itertools
from pathlib import Path
from flask import Flask, Response, render_template, jsonify
from pydub import AudioSegment

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent
MUSIC_FOLDER = BASE_DIR / "Music"

SUPPORTED_FORMATS = [".mp3", ".wav", ".ogg", ".m4a"]

current_song = {"name": "Iniciando transmisión..."}


def load_music():
    files = []
    for ext in SUPPORTED_FORMATS:
        files.extend(MUSIC_FOLDER.rglob(f"*{ext}"))
    return list(files)


def radio_stream():
    songs = load_music()

    if not songs:
        yield b""
        return

    while True:
        random.shuffle(songs)

        for song_path in songs:
            try:
                current_song["name"] = song_path.stem

                audio = AudioSegment.from_file(song_path)

                audio = audio.normalize()

                audio = audio.set_frame_rate(44100).set_channels(2)

                chunk_size = 4096
                raw_data = audio.raw_data

                for i in range(0, len(raw_data), chunk_size):
                    yield raw_data[i:i + chunk_size]

            except Exception:
                continue


def get_song_names():
    songs = load_music()
    return [song.stem for song in songs]


@app.route("/")
def index():
    return render_template("index.html", song_names=get_song_names())


@app.route("/stream")
def stream():
    return Response(radio_stream(), mimetype="audio/mpeg")


@app.route("/now-playing")
def now_playing():
    return jsonify(current_song)


if __name__ == "__main__":
    print("Radio en vivo en http://127.0.0.1:8080/")
    app.run(host="0.0.0.0", port=8080, threaded=True)
