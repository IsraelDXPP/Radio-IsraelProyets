import os
from flask import Flask, Response, render_template, send_from_directory
import threading
from queue import Queue
from pathlib import Path
from pydub import AudioSegment

app = Flask(__name__)

# Carpeta donde se encuentra la música
audio_folder = os.path.join(os.path.dirname(__file__), 'Music')

playlist = Queue()

def load_playlist():
    # Cargar la lista de reproducción al inicio del programa
    ogg_files = list(Path(audio_folder).rglob('*.ogg'))
    for ogg_file in ogg_files:
        # Convertir archivos .ogg a .wav utilizando pydub
        wav_file = os.path.splitext(ogg_file)[0] + '.wav'
        audio = AudioSegment.from_ogg(ogg_file)
        audio.export(wav_file, format="wav")
        playlist.put_nowait([wav_file])

    playlist.put_nowait(list(Path(audio_folder).rglob('*.mp3')))
    playlist.put_nowait(list(Path(audio_folder).rglob('*.m4a')))
    playlist.put_nowait(list(Path(audio_folder).rglob('*.wav')))

def generate_audio():
    load_playlist()
    while not playlist.empty():
        file_paths = playlist.get_nowait()
        for file_path in file_paths:
            with open(file_path, 'rb') as audio_file:
                yield audio_file.read()

def get_song_names():
    return [path.stem for path in Path(audio_folder).rglob('*.mp3')]

@app.route('/')
def index():
    song_names = get_song_names()
    return render_template('index.html', song_names=song_names)

@app.route('/stream')
def stream():
    return Response(generate_audio(), mimetype='audio/mpeg')

@app.route('/static/<path:filename>')
def custom_static(filename):
    return send_from_directory('static', filename)

@app.route('/templates/Png/<path:filename>')
def custom_template_static(filename):
    return send_from_directory('templates/Png', filename)

def cleanup():
    print("\nCerrando el servidor...")
    os._exit(0)

if __name__ == '__main__':
    print("El servidor está en ejecución. Abre tu navegador y visita http://127.0.0.1:8080/")
    threading.Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': 8080, 'threaded': True}).start()
    
    try:
        # Mantener el programa en ejecución hasta que se interrumpa con Ctrl + C
        while True:
            pass
    except KeyboardInterrupt:
        cleanup()
