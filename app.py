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
import ftplib
import json
import os
import shutil
import socket
import ssl
import subprocess
import threading
import time
from queue import Queue, Empty, Full

import paho.mqtt.client as mqtt
from flask import Flask, Response, jsonify, render_template, request, send_from_directory

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


def _int_or_none(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _float_or_none(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# Bambu firmware's print stage codes. -1 / 255 mean "no active stage".
_STAGE_NAMES = {
    0: "Printing",
    1: "Auto bed leveling",
    2: "Heatbed preheating",
    3: "Sweeping XY mech",
    4: "Changing filament",
    5: "M400 pause",
    6: "Paused: filament runout",
    7: "Heating hotend",
    8: "Calibrating extrusion",
    9: "Scanning bed surface",
    10: "Inspecting first layer",
    11: "Identifying build plate",
    12: "Calibrating Micro Lidar",
    13: "Homing toolhead",
    14: "Cleaning nozzle",
    15: "Checking extruder temp",
    16: "Paused by user",
    17: "Paused: front cover",
    18: "Calibrating Micro Lidar",
    19: "Calibrating extrusion flow",
    20: "Paused: nozzle temp malfunction",
    21: "Paused: bed temp malfunction",
    22: "Filament unloading",
    23: "Skip-step pause",
    24: "Filament loading",
    25: "Motor noise calibration",
    26: "Paused: AMS lost",
    27: "Paused: low fan speed (heat break)",
    28: "Paused: chamber temp control error",
    29: "Cooling chamber",
    30: "Paused: user-triggered gcode",
    31: "Motor noise showoff",
    32: "Nozzle filament covered",
    33: "Cutter error",
    34: "First layer error",
    35: "Nozzle clog",
}


def _stage_label(code):
    if code is None:
        return None
    if code in (-1, 255):
        return None
    return _STAGE_NAMES.get(code, f"Stage {code}")


def _nozzle_temps(p):
    """Return a list of {'temp': float|None, 'target': float|None} per nozzle.
    Tries dual-nozzle H2D layouts first, falls back to the single-nozzle fields.
    """
    # H2D dual-head layouts — Bambu has used a few variations; try in order.
    candidates = (
        p.get("device", {}).get("nozzle", {}).get("info"),
        p.get("device", {}).get("nozzle", {}).get("state"),
        p.get("device", {}).get("head"),
    )
    for arr in candidates:
        if isinstance(arr, list) and len(arr) >= 2:
            out = []
            for item in arr:
                if not isinstance(item, dict):
                    continue
                out.append({
                    "temp": _float_or_none(
                        item.get("temp") or item.get("nozzle_temper")
                        or item.get("now")
                    ),
                    "target": _float_or_none(
                        item.get("target_temp") or item.get("nozzle_target_temper")
                        or item.get("target")
                    ),
                })
            if out:
                return out

    # Flat dual-nozzle fields some firmwares emit.
    if "nozzle_temper_1" in p or "nozzle_target_temper_1" in p:
        return [
            {"temp": _float_or_none(p.get("nozzle_temper")),
             "target": _float_or_none(p.get("nozzle_target_temper"))},
            {"temp": _float_or_none(p.get("nozzle_temper_1")),
             "target": _float_or_none(p.get("nozzle_target_temper_1"))},
        ]

    # Single-nozzle fallback (X1, P1, A1, single-head H2D mode).
    return [{
        "temp": _float_or_none(p.get("nozzle_temper")),
        "target": _float_or_none(p.get("nozzle_target_temper")),
    }]


def _summarize_tray(tray):
    color = (tray.get("tray_color") or "").strip()
    rgb = color[:6] if len(color) >= 6 else None  # strip alpha if "RRGGBBAA"
    ttype = (tray.get("tray_type") or "").strip()
    return {
        "id": _int_or_none(tray.get("id")),
        "type": ttype or None,
        "color": f"#{rgb}" if rgb and rgb != "000000" else None,
        "remain": _int_or_none(tray.get("remain")),  # -1 or 0..100
        "empty": not ttype,
    }


def _summarize_ams(p):
    ams_root = p.get("ams") or {}
    units = []
    for unit in ams_root.get("ams") or []:
        units.append({
            "id": _int_or_none(unit.get("id")),
            "humidity": _int_or_none(unit.get("humidity")),
            "temp": _int_or_none(float(unit["temp"])) if unit.get("temp") not in (None, "") else None,
            "trays": [_summarize_tray(t) for t in unit.get("tray") or []],
        })
    vt = p.get("vt_tray")
    return {
        "units": units,
        "active_tray": ams_root.get("tray_now"),  # global tray id as string
        "external": _summarize_tray(vt) if isinstance(vt, dict) else None,
    }


def _summarize(raw):
    p = raw.get("print", {}) if isinstance(raw, dict) else {}
    nozzles = _nozzle_temps(p)
    return {
        "state": p.get("gcode_state"),
        "stage": _int_or_none(p.get("stg_cur")),
        "stage_label": _stage_label(_int_or_none(p.get("stg_cur"))),
        "progress": p.get("mc_percent"),
        "remaining_min": p.get("mc_remaining_time"),
        "layer": p.get("layer_num"),
        "total_layers": p.get("total_layer_num"),
        "job": p.get("subtask_name") or p.get("gcode_file"),
        # Back-compat single-nozzle fields (first entry).
        "nozzle_temp": nozzles[0]["temp"] if nozzles else None,
        "nozzle_target": nozzles[0]["target"] if nozzles else None,
        # Dual-head: list of {temp, target}.
        "nozzles": nozzles,
        "bed_temp": p.get("bed_temper"),
        "bed_target": p.get("bed_target_temper"),
        "chamber_temp": p.get("chamber_temper"),
        "speed_level": p.get("spd_lvl"),
        "wifi": p.get("wifi_signal"),
        "error": p.get("print_error"),
        "ams": _summarize_ams(p),
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


# ----- Timelapse sync (FTPS pull from printer's SD card) -----
# Bambu printers expose vsFTPd on :990 (implicit TLS). Auth uses bblp + access
# code. Timelapses live under /timelapse on the SD card. We periodically scan
# and pull anything we don't have a complete local copy of.
TIMELAPSE_DIR = os.environ.get("TIMELAPSE_DIR", "/timelapses")
TIMELAPSE_REMOTE_DIR = os.environ.get("TIMELAPSE_REMOTE_DIR", "/timelapse")
TIMELAPSE_POLL_SECONDS = int(os.environ.get("TIMELAPSE_POLL_SECONDS", "300"))


class _ImplicitFTPS(ftplib.FTP_TLS):
    """Bambu's vsFTPd requires (a) implicit TLS on port 990 and (b) data-channel
    TLS sessions to be reused from the control channel (`522 session reuse
    required`). Stock ftplib.FTP_TLS does neither.
    """

    def connect(self, host="", port=0, timeout=-999, source_address=None):
        if host:
            self.host = host
        if port:
            self.port = port
        if timeout != -999:
            self.timeout = timeout
        self.sock = socket.create_connection((self.host, self.port), self.timeout)
        self.af = self.sock.family
        self.sock = self.context.wrap_socket(self.sock, server_hostname=self.host)
        self.file = self.sock.makefile("r")
        self.welcome = self.getresp()
        return self.welcome

    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(
                conn,
                server_hostname=self.host,
                session=self.sock.session,
            )
        return conn, size


def _parse_mlsd(line):
    """Parse one MLSD line: 'type=file;size=12345;modify=...; filename.mp4'."""
    parts = line.rsplit("; ", 1)
    if len(parts) != 2:
        return None
    meta_raw, name = parts
    meta = {}
    for kv in meta_raw.split(";"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            meta[k.lower()] = v
    return name, meta


def _list_entries(ftp):
    """Return [(name, kind, size)] for the current FTP directory.
    kind is 'file' or 'dir'. Tries MLSD first, falls back to NLST + SIZE probe.
    """
    lines = []
    try:
        ftp.retrlines("MLSD", lines.append)
    except ftplib.error_perm:
        lines = None

    if lines is not None:
        out = []
        for line in lines:
            parsed = _parse_mlsd(line)
            if not parsed:
                continue
            name, meta = parsed
            if name in (".", ".."):
                continue
            t = meta.get("type", "")
            if t in ("cdir", "pdir"):
                continue
            kind = "dir" if t == "dir" else ("file" if t == "file" else None)
            if kind is None:
                continue
            size = int(meta["size"]) if "size" in meta else None
            out.append((name, kind, size))
        return out

    # NLST fallback: probe each entry with SIZE — files return a size, dirs 550.
    names = []
    ftp.retrlines("NLST", names.append)
    out = []
    for name in names:
        if not name or name in (".", ".."):
            continue
        try:
            size = ftp.size(name)
            out.append((name, "file", size))
        except ftplib.error_perm:
            out.append((name, "dir", None))
    return out


def _download_file(ftp, name, local_path, size):
    if os.path.exists(local_path):
        if size is None or os.path.getsize(local_path) == size:
            return
    tmp = local_path + ".part"
    print(f"[timelapse] downloading {local_path}" + (f" ({size} bytes)" if size else ""))
    try:
        with open(tmp, "wb") as f:
            ftp.retrbinary(f"RETR {name}", f.write)
        os.rename(tmp, local_path)
    except Exception as e:
        print(f"[timelapse] failed {local_path}: {e!r}")
        try:
            os.unlink(tmp)
        except OSError:
            pass


def _walk_and_sync(ftp, remote_dir, local_dir, depth=0):
    if depth > 4:
        return  # safety cap; Bambu's tree is just /timelapse and /timelapse/thumbnail
    try:
        ftp.cwd(remote_dir)
    except ftplib.error_perm:
        return  # remote dir doesn't exist (no captures yet)
    os.makedirs(local_dir, exist_ok=True)
    for name, kind, size in _list_entries(ftp):
        sub_remote = f"{remote_dir.rstrip('/')}/{name}"
        sub_local = os.path.join(local_dir, name)
        if kind == "file":
            _download_file(ftp, name, sub_local, size)
        elif kind == "dir":
            _walk_and_sync(ftp, sub_remote, sub_local, depth + 1)
            ftp.cwd(remote_dir)  # restore CWD after recursion


def _sync_timelapses():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    ftp = _ImplicitFTPS(context=ctx)
    ftp.connect(PRINTER_IP, 990, timeout=20)
    ftp.login("bblp", PRINTER_ACCESS_CODE)
    ftp.prot_p()  # encrypt data channel too
    try:
        _walk_and_sync(ftp, TIMELAPSE_REMOTE_DIR, TIMELAPSE_DIR)
    finally:
        try:
            ftp.quit()
        except Exception:
            try:
                ftp.close()
            except Exception:
                pass


def _timelapse_loop():
    while True:
        try:
            _sync_timelapses()
        except Exception as e:
            print(f"[timelapse] {e!r}")
        time.sleep(TIMELAPSE_POLL_SECONDS)


# ----- Camera (RTSP/RTSPS via ffmpeg) -----
# Default: direct to the printer's RTSPS endpoint on port 322. Requires Developer
# Mode + LAN Only Mode on the printer.
# Override: set CAMERA_URL to point at a relay (e.g. MediaMTX on a Mac mini
# running Bambu Studio Go Live) and the printer can stay in cloud mode.
CAM_PORT = int(os.environ.get("CAMERA_PORT", "322"))
CAM_PATH = os.environ.get("CAMERA_PATH", "/streaming/live/1")
CAM_FPS = int(os.environ.get("CAMERA_FPS", "10"))
CAM_URL = os.environ.get("CAMERA_URL", "")


def _rtsp_url():
    if CAM_URL:
        return CAM_URL
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


VIDEO_EXTS = (".mp4", ".avi", ".mkv", ".mov")
THUMB_EXTS = (".jpg", ".jpeg", ".png", ".webp")


def _safe_video_name(name):
    """Validate a video filename came from the timelapses dir, not a traversal."""
    if not name or "/" in name or "\\" in name or ".." in name:
        return None
    if not name.lower().endswith(VIDEO_EXTS):
        return None
    if not TIMELAPSE_DIR or not os.path.isfile(os.path.join(TIMELAPSE_DIR, name)):
        return None
    return name


def _sidecar_path(name):
    return os.path.join(TIMELAPSE_DIR, name + ".json")


def _read_sidecar(name):
    try:
        with open(_sidecar_path(name), "r") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _write_sidecar(name, data):
    path = _sidecar_path(name)
    if data:
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    else:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


@app.route("/api/timelapses")
def api_timelapses():
    if not TIMELAPSE_DIR or not os.path.isdir(TIMELAPSE_DIR):
        return jsonify({"items": []})
    thumb_dir = os.path.join(TIMELAPSE_DIR, "thumbnail")
    thumbs = {}
    if os.path.isdir(thumb_dir):
        for fname in os.listdir(thumb_dir):
            base, ext = os.path.splitext(fname)
            if ext.lower() in THUMB_EXTS:
                thumbs[base] = fname
    items = []
    for entry in os.scandir(TIMELAPSE_DIR):
        if not entry.is_file():
            continue
        if not entry.name.lower().endswith(VIDEO_EXTS):
            continue
        base = os.path.splitext(entry.name)[0]
        stat = entry.stat()
        meta = _read_sidecar(entry.name)
        items.append({
            "name": entry.name,
            "size": stat.st_size,
            "mtime": stat.st_mtime,
            "thumbnail": thumbs.get(base),
            "url": meta.get("url"),
        })
    items.sort(key=lambda i: i["mtime"], reverse=True)
    return jsonify({"items": items})


@app.route("/api/timelapses/<name>/url", methods=["POST"])
def set_timelapse_url(name):
    safe = _safe_video_name(name)
    if not safe:
        return jsonify({"error": "invalid name"}), 400
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    if url:
        if not url.lower().startswith(("http://", "https://")):
            return jsonify({"error": "url must start with http:// or https://"}), 400
        if len(url) > 2048:
            return jsonify({"error": "url too long"}), 400
    existing = _read_sidecar(safe)
    if url:
        existing["url"] = url
    else:
        existing.pop("url", None)
    _write_sidecar(safe, existing)
    return jsonify({"ok": True, "url": url or None})


@app.route("/timelapses/<path:filename>")
def serve_timelapse(filename):
    if not TIMELAPSE_DIR:
        return "", 404
    # send_from_directory blocks ../ traversal and supports HTTP Range,
    # which the browser needs to seek inside <video> elements.
    return send_from_directory(TIMELAPSE_DIR, filename, conditional=True)


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
if TIMELAPSE_POLL_SECONDS > 0 and TIMELAPSE_DIR:
    threading.Thread(target=_timelapse_loop, daemon=True).start()


if __name__ == "__main__":
    app.run(host=HOST, port=PORT, threaded=True)
