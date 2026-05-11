"""Bambu H2D dashboard: live chamber camera + print status.

Setup:
    sudo apt install ffmpeg          # H2D camera is RTSPS, ffmpeg does the decode
    pip install -r requirements.txt
    export PRINTER_IP=192.168.x.x
    export PRINTER_SERIAL=<serial from printer screen>
    export PRINTER_ACCESS_CODE=<8-digit code from printer screen>
    python app.py

Then open http://localhost:5000

On the H2D you must enable BOTH:
  - LAN Liveview     (enables the RTSPS camera endpoint)
  - Developer Mode   (opens port 322 on the printer for LAN clients)
Otherwise port 322 stays closed and ffmpeg will fail to connect.
"""
import json
import os
import shutil
import ssl
import subprocess
import threading
import time
from queue import Queue, Empty, Full

import paho.mqtt.client as mqtt
from flask import Flask, Response, jsonify, render_template

PRINTER_IP = os.environ.get("PRINTER_IP", "")
PRINTER_SERIAL = os.environ.get("PRINTER_SERIAL", "")
PRINTER_ACCESS_CODE = os.environ.get("PRINTER_ACCESS_CODE", "")
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "5000"))

app = Flask(__name__)

# ----- shared state -----
_status_lock = threading.Lock()
_status = {"connected": False, "raw": {}}


def _merge(dst, src):
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _merge(dst[k], v)
        else:
            dst[k] = v


def _summarize(raw):
    p = raw.get("print", {}) if isinstance(raw, dict) else {}
    return {
        "state": p.get("gcode_state"),
        "progress": p.get("mc_percent"),
        "remaining_min": p.get("mc_remaining_time"),
        "layer": p.get("layer_num"),
        "total_layers": p.get("total_layer_num"),
        "job": p.get("subtask_name") or p.get("gcode_file"),
        "nozzle_temp": p.get("nozzle_temper"),
        "nozzle_target": p.get("nozzle_target_temper"),
        "bed_temp": p.get("bed_temper"),
        "bed_target": p.get("bed_target_temper"),
        "chamber_temp": p.get("chamber_temper"),
        "speed_level": p.get("spd_lvl"),
        "wifi": p.get("wifi_signal"),
        "error": p.get("print_error"),
    }


# ----- MQTT (print status) -----
def _mqtt_loop():
    topic = f"device/{PRINTER_SERIAL}/report"
    request_topic = f"device/{PRINTER_SERIAL}/request"

    def on_connect(client, userdata, flags, reason_code, properties=None):
        with _status_lock:
            _status["connected"] = True
        client.subscribe(topic)
        # Ask for a full status snapshot so we don't wait for the next push.
        client.publish(request_topic, json.dumps({
            "pushing": {"sequence_id": "0", "command": "pushall"}
        }))

    def on_disconnect(client, userdata, *args, **kwargs):
        with _status_lock:
            _status["connected"] = False

    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return
        with _status_lock:
            _merge(_status["raw"], payload)

    while True:
        try:
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"bambu-dash-{os.getpid()}",
            )
            client.username_pw_set("bblp", PRINTER_ACCESS_CODE)
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            client.tls_set_context(ctx)
            client.on_connect = on_connect
            client.on_disconnect = on_disconnect
            client.on_message = on_message
            client.connect(PRINTER_IP, 8883, keepalive=60)
            client.loop_forever()
        except Exception as e:
            with _status_lock:
                _status["connected"] = False
            print(f"[mqtt] {e!r}; reconnecting in 5s")
            time.sleep(5)


# ----- Camera (RTSPS on port 322, via ffmpeg) -----
# H2D exposes its chamber camera as RTSPS. Requires Developer Mode + LAN Liveview
# both enabled on the printer. We let ffmpeg handle TLS, RTSP, and H.264/H.265
# decode, then take its MJPEG output and fan it out to /stream.mjpg subscribers.
CAM_PORT = int(os.environ.get("CAMERA_PORT", "322"))
CAM_PATH = os.environ.get("CAMERA_PATH", "/streaming/live/1")
CAM_FPS = int(os.environ.get("CAMERA_FPS", "10"))


def _rtsp_url():
    return f"rtsps://bblp:{PRINTER_ACCESS_CODE}@{PRINTER_IP}:{CAM_PORT}{CAM_PATH}"


class CameraBroker:
    """One ffmpeg subprocess feeding MJPEG to many subscribers."""

    def __init__(self):
        self._subscribers = []
        self._lock = threading.Lock()
        self._thread = None
        self._latest = None

    def _ensure_running(self):
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run, daemon=True)
                self._thread.start()

    def subscribe(self):
        q = Queue(maxsize=4)
        with self._lock:
            self._subscribers.append(q)
            latest = self._latest
        self._ensure_running()
        if latest is not None:
            try:
                q.put_nowait(latest)
            except Full:
                pass
        return q

    def unsubscribe(self, q):
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def _publish(self, frame):
        with self._lock:
            self._latest = frame
            subs = list(self._subscribers)
        for q in subs:
            try:
                q.put_nowait(frame)
            except Full:
                try:
                    q.get_nowait()
                except Empty:
                    pass
                try:
                    q.put_nowait(frame)
                except Full:
                    pass

    def _run(self):
        while True:
            with self._lock:
                if not self._subscribers:
                    self._thread = None
                    return
            try:
                self._read_stream()
            except FileNotFoundError:
                print("[camera] ffmpeg not found in PATH; install ffmpeg to view camera")
                time.sleep(10)
            except Exception as e:
                print(f"[camera] {e!r}; reconnecting in 3s")
                time.sleep(3)

    def _read_stream(self):
        if not shutil.which("ffmpeg"):
            raise FileNotFoundError("ffmpeg")
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            "-rtsp_transport", "tcp",
            "-allowed_media_types", "video",
            "-i", _rtsp_url(),
            "-an",
            "-f", "image2pipe",
            "-c:v", "mjpeg",
            "-q:v", "5",
            "-r", str(CAM_FPS),
            "pipe:1",
        ]
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0,
        )
        threading.Thread(
            target=self._drain_stderr, args=(proc,), daemon=True,
        ).start()
        try:
            self._parse_mjpeg(proc.stdout)
        finally:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except Exception:
                pass

    def _drain_stderr(self, proc):
        for raw in iter(proc.stderr.readline, b""):
            line = raw.decode(errors="replace").rstrip()
            if line:
                print(f"[ffmpeg] {line}")

    def _parse_mjpeg(self, stream):
        buf = bytearray()
        SOI = b"\xff\xd8\xff"
        EOI = b"\xff\xd9"
        while True:
            with self._lock:
                if not self._subscribers:
                    return
            chunk = stream.read(65536)
            if not chunk:
                raise ConnectionError("ffmpeg exited")
            buf += chunk
            while True:
                soi = buf.find(SOI)
                if soi < 0:
                    if len(buf) > 1 << 20:
                        del buf[: -len(SOI)]
                    break
                eoi = buf.find(EOI, soi + len(SOI))
                if eoi < 0:
                    if soi > 0:
                        del buf[:soi]
                    break
                frame = bytes(buf[soi : eoi + len(EOI)])
                del buf[: eoi + len(EOI)]
                self._publish(frame)


camera = CameraBroker()


# ----- Flask routes -----
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/healthz")
def healthz():
    return "ok", 200


@app.route("/api/status")
def api_status():
    with _status_lock:
        return jsonify({
            "connected": _status["connected"],
            "summary": _summarize(_status["raw"]),
            "raw": _status["raw"],
        })


@app.route("/stream.mjpg")
def stream():
    boundary = b"--frame"

    def gen():
        q = camera.subscribe()
        try:
            while True:
                try:
                    frame = q.get(timeout=15)
                except Empty:
                    continue
                yield (
                    boundary + b"\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Content-Length: " + str(len(frame)).encode() + b"\r\n\r\n"
                    + frame + b"\r\n"
                )
        finally:
            camera.unsubscribe(q)

    return Response(
        gen(),
        mimetype="multipart/x-mixed-replace; boundary=frame",
        headers={"Cache-Control": "no-store"},
    )


_missing = [k for k in ("PRINTER_IP", "PRINTER_SERIAL", "PRINTER_ACCESS_CODE")
            if not os.environ.get(k)]
if _missing:
    raise SystemExit(f"Missing env vars: {', '.join(_missing)} (see .env.example)")
threading.Thread(target=_mqtt_loop, daemon=True).start()


if __name__ == "__main__":
    app.run(host=HOST, port=PORT, threaded=True)
