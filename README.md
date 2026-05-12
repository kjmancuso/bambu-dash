# bambu-dash

A small Flask app that displays the live chamber camera and print status
of a Bambu Lab **H2D** printer on the local network.

- Live MJPEG stream of the chamber camera (RTSPS → transcoded by ffmpeg)
- Real-time print job status (state, progress, layer count, temps, errors)
- Single page, dark UI, no JS framework

## Printer prerequisites

Three toggles on the H2D touchscreen must be on:

1. **Settings → LAN Only mode** — required for the camera endpoint and to
   reveal Developer Mode. The printer becomes unreachable from Bambu cloud /
   Handy app while this is on, but stays fully usable on the LAN. You can
   toggle it off whenever you want cloud features back.
2. **Settings → LAN Only Liveview** — opens the RTSPS camera endpoint on
   port 322.
3. **Settings → Developer Mode** (appears after LAN Only mode is enabled;
   scroll to the bottom of the warning to enable) — opens MQTT and TLS
   services to LAN clients without per-action authorization prompts.

You will also need:

- **Printer IP** (Settings → WLAN)
- **Serial number** (Settings → Device Info)
- **Access code** (Settings → WLAN → tap your network → Access Code)

If you don't want to give up cloud features, see
[Camera relay via a host (optional)](#camera-relay-via-a-host-optional) for an
alternative that keeps the printer in cloud mode.

## Camera relay via a host (optional)

If you have an always-on machine (e.g. a Mac mini) running Bambu Studio, you
can use **Bambu Studio's "Go Live"** feature + a small relay to serve the
camera over plain RTSP. The container then connects to the relay instead of
directly to the printer, so the H2D can stay in cloud mode (Bambu Handy app,
cloud monitoring, etc. all keep working).

Trade-offs:

- ✅ Printer keeps full cloud connectivity
- ✅ No Developer Mode / LAN Only Mode required for the camera
- ❌ Adds a host dependency: Bambu Studio's background camera process must
  stay running
- ❌ One more hop in the video path

### Setup (macOS, with Bambu Studio + MediaMTX)

1. **Enable Go Live in Bambu Studio.** Select the printer → click the
   camera-settings icon (top-right of the video panel) → toggle **Go Live**
   on. Bambu Studio writes
   `~/Library/Application Support/BambuStudio/cameratools/ffmpeg.sdp` and
   spawns a background camera process that persists after you quit the GUI.

2. **Install MediaMTX** (a multi-protocol streaming relay):

   ```sh
   brew install mediamtx
   ```

3. **Write the config to where brew's service expects it**
   (`/opt/homebrew/etc/mediamtx/mediamtx.yml` on Apple Silicon — `brew services info
   mediamtx` shows the exact path):

   ```yaml
   paths:
     printer:
       runOnInit: >
         /opt/homebrew/bin/ffmpeg
         -protocol_whitelist file,rtp,udp
         -i "/Users/YOU/Library/Application Support/BambuStudio/cameratools/ffmpeg.sdp"
         -c copy
         -f rtsp rtsp://localhost:8554/printer
       runOnInitRestart: yes
   ```

   Replace `YOU` with your username. `runOnInitRestart: yes` brings ffmpeg
   back if Bambu Studio cycles the camera.

4. **Start the service:**

   ```sh
   brew services start mediamtx
   ```

5. **Verify** from any host on your LAN:

   ```sh
   ffplay rtsp://<macmini-host>:8554/printer
   ```

   Initial H.264 "concealing N errors" lines are normal — RTSP clients have
   to wait for the next keyframe.

6. **Point bambu-dash at the relay** by setting `CAMERA_URL`:

   ```sh
   docker run … -e CAMERA_URL=rtsp://<macmini-host>:8554/printer …
   ```

   In Kubernetes, add `CAMERA_URL` to the env on the Deployment.

7. **Optional: undo the printer hardening.** Once the relay is working, you
   can turn off Developer Mode and LAN Only Mode on the H2D — the camera
   keeps working via the relay. (`PRINTER_IP`, `PRINTER_SERIAL`,
   `PRINTER_ACCESS_CODE` are still required for MQTT print status and
   timelapse FTPS pulls, which work in cloud mode as long as the container
   can reach the printer on the LAN.)

### Common pitfall

`brew services` ignores `~/.config/mediamtx/mediamtx.yml` and uses
`/opt/homebrew/etc/mediamtx/mediamtx.yml` instead. If `ffplay rtsp://…:8554/printer`
returns `404 Not Found`, double-check that the config above is at the
brew-managed path — not somewhere else.

## Run locally

Requires Python 3.12+ and `ffmpeg` on `PATH`.

```sh
sudo apt install ffmpeg
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

export PRINTER_IP=192.168.x.x
export PRINTER_SERIAL=00M00A000000000
export PRINTER_ACCESS_CODE=12345678

python app.py
```

Open <http://localhost:5000>.

## Run as a container

```sh
docker build -t bambu-dash .

docker run --rm -p 5000:5000 \
  -e PRINTER_IP=192.168.x.x \
  -e PRINTER_SERIAL=00M00A000000000 \
  -e PRINTER_ACCESS_CODE=12345678 \
  bambu-dash
```

The image runs `gunicorn` (single worker, gthread, `--timeout 0` for the
long-lived MJPEG response) on port 5000 as UID 1000.

## CI / image publishing

[`.github/workflows/docker.yml`](.github/workflows/docker.yml) publishes
`ghcr.io/<owner>/<repo>` on push to `main` and on `v*` tags. Tags applied:

- `main` → `:main`, `:sha-<…>`, `:latest`
- `v1.2.3` tag → `:1.2.3`, `:1.2`, `:sha-<…>`
- PRs build but don't push

## Deploying to Kubernetes

There are no manifests in this repo — they live in a separate Argo
repository. The image contract you need to match:

| Field            | Value                                                                |
| ---------------- | -------------------------------------------------------------------- |
| Container port   | `5000` (HTTP)                                                        |
| Required env     | `PRINTER_IP`, `PRINTER_SERIAL`, `PRINTER_ACCESS_CODE`                |
| Optional env     | `CAMERA_URL` (overrides auto URL — point at a relay), `CAMERA_PORT` (322), `CAMERA_PATH` (`/streaming/live/1`), `CAMERA_FPS` (10), `TIMELAPSE_DIR` (`/timelapses`, set to `""` to disable), `TIMELAPSE_REMOTE_DIR` (`/timelapse`), `TIMELAPSE_POLL_SECONDS` (300) |
| Liveness         | `GET /healthz` → 200                                                 |
| Readiness        | `GET /healthz` → 200                                                 |
| User             | UID 1000, non-root                                                   |
| Replicas         | **1** (one process owns the upstream camera + MQTT connections)      |
| Update strategy  | `Recreate` (avoid two pods fighting for the camera stream)           |

If you set `readOnlyRootFilesystem: true`, mount an `emptyDir` at
`/home/app` so gunicorn can create its control socket
(`/home/app/.gunicorn/gunicorn.ctl`).

To persist timelapses across pod restarts, mount a `PersistentVolumeClaim`
at `/timelapses`. The container pulls new files from the printer's FTPS
server every `TIMELAPSE_POLL_SECONDS` seconds; files already present
locally with matching size are skipped.

The pod must be able to route to the printer's IP. If your CNI doesn't
bridge to the LAN by default, `hostNetwork: true` works.

## Endpoints

| Path           | Description                                            |
| -------------- | ------------------------------------------------------ |
| `/`            | Dashboard HTML                                         |
| `/api/status`  | JSON: `{ connected, summary, raw }` from printer MQTT     |
| `/stream.mjpg` | `multipart/x-mixed-replace` MJPEG of the chamber cam   |
| `/healthz`     | `"ok"` 200 (for liveness/readiness probes)             |

## How it works

- **Status:** a background thread holds an MQTT-TLS connection to the
  printer on `:8883` (user `bblp`, password = access code), subscribes
  to `device/<serial>/report`, merges incremental updates into in-memory
  state, and exposes it via `/api/status`.
- **Camera:** when the first MJPEG subscriber connects, a single ffmpeg
  subprocess opens `rtsps://bblp:<code>@<ip>:322/streaming/live/1`,
  transcodes to MJPEG on stdout, and a `CameraBroker` fans frames out to
  every connected client. Slow clients are dropped per-frame, not
  per-stream.
- **Single process by design:** running multiple replicas would mean
  multiple ffmpeg subprocesses competing for the camera and multiple MQTT
  clients receiving duplicate state, with no upside. Keep `replicas: 1`.
