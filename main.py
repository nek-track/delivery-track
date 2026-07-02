"""
ระบบติดตามรถส่งของแบบเรียลไทม์ (Delivery Tracker)
- รับพิกัดจากแอป Traccar Client (โปรโตคอล OsmAnd) บนมือถือ 2 เครื่อง
- แอดมินสร้าง "งานส่งของ" ผูกกับมือถือ แล้วได้ลิงก์ tracking ส่งให้ลูกค้า
- ลูกค้าเปิดลิงก์ดูตำแหน่งบนแผนที่แบบเรียลไทม์ ลิงก์หมดอายุเมื่อจบงาน

รันในเครื่อง:  uvicorn main:app --host 0.0.0.0 --port 8000
Deploy Render: start command = uvicorn main:app --host 0.0.0.0 --port $PORT
"""

import json
import os
import secrets
import sqlite3
import time
from contextlib import contextmanager
from math import asin, cos, radians, sin, sqrt
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

# ------------------------------------------------------------------ config

ADMIN_KEY = os.environ.get("ADMIN_KEY", "changeme")   # ตั้งใน Render env var
DB_PATH = os.environ.get("DB_PATH", "tracker.db")
JOB_TTL_HOURS = 24            # งานหมดอายุอัตโนมัติหลังสร้าง (ชั่วโมง)
STALE_SECONDS = 180           # เกินกี่วินาทีถือว่าสัญญาณขาด (Traccar ตั้งส่งทุก 60 วิ = เผื่อ 3 รอบ)
TRACK_MIN_METERS = 15         # ขยับน้อยกว่านี้ไม่บันทึกจุดใหม่ (กันจุดซ้อนตอนรถจอด)
TRACK_RETENTION_HOURS = 72    # เก็บประวัติ track ย้อนหลังกี่ชั่วโมง (ล้างของเก่าอัตโนมัติ)

# มือถือ 2 เครื่อง — ตั้ง Device identifier ในแอป Traccar Client ให้ตรงกับ id ตรงนี้
DEVICES = {
    "phone1": "มือถือเครื่องที่ 1",
    "phone2": "มือถือเครื่องที่ 2",
}

BASE_DIR = Path(__file__).parent
app = FastAPI(title="Delivery Tracker", docs_url=None, redoc_url=None)

# ------------------------------------------------------------------ database


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS positions (
            device_id TEXT PRIMARY KEY,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            speed_kmh REAL DEFAULT 0,
            bearing REAL DEFAULT 0,
            battery REAL,
            updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS jobs (
            token TEXT PRIMARY KEY,
            device_id TEXT NOT NULL,
            customer TEXT NOT NULL,
            destination TEXT DEFAULT '',
            dest_lat REAL,
            dest_lon REAL,
            status TEXT DEFAULT 'active',        -- active | done
            created_at INTEGER NOT NULL,
            ended_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            speed_kmh REAL DEFAULT 0,
            recorded_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tracks_dev_time
            ON tracks(device_id, recorded_at);
        """)


init_db()


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2 - lat1) / 2) ** 2 + cos(lat1) * cos(lat2) * sin((lon2 - lon1) / 2) ** 2
    return 6371 * 2 * asin(sqrt(a))


def require_admin(key: str):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="invalid admin key")


def render(name: str, **vars) -> str:
    # รองรับทั้งกรณีไฟล์อยู่ในโฟลเดอร์ templates/ และวางข้าง main.py
    for candidate in (BASE_DIR / "templates" / name, BASE_DIR / name):
        if candidate.exists():
            html = candidate.read_text(encoding="utf-8")
            break
    else:
        raise HTTPException(status_code=500, detail=f"template not found: {name}")
    for k, v in vars.items():
        html = html.replace("{{" + k + "}}", str(v))
    return html


# ---------------------------------------------------- 1) รับพิกัดจาก Traccar Client
# Traccar Client (OsmAnd protocol) ยิงมาที่ / ด้วย GET หรือ POST
# พารามิเตอร์: id, lat, lon, timestamp, speed (น็อต), bearing, batt


@app.get("/")
@app.post("/")
async def ingest(request: Request):
    p = dict(request.query_params)
    if request.method == "POST":
        try:
            form = await request.form()
            p.update(dict(form))
        except Exception:
            pass

    device_id = p.get("id")
    lat, lon = p.get("lat"), p.get("lon")
    if not device_id or lat is None or lon is None:
        # เปิดหน้าแรกด้วย browser ธรรมดา
        return PlainTextResponse("Delivery Tracker is running")

    if device_id not in DEVICES:
        raise HTTPException(status_code=400, detail="unknown device id")

    lat_f, lon_f = float(lat), float(lon)
    speed_knots = float(p.get("speed", 0) or 0)
    speed_kmh = round(speed_knots * 1.852, 1)          # knots -> km/h
    now = int(time.time())
    with db() as conn:
        prev = conn.execute(
            "SELECT lat, lon FROM positions WHERE device_id=?", (device_id,)
        ).fetchone()

        conn.execute(
            """INSERT INTO positions (device_id, lat, lon, speed_kmh, bearing, battery, updated_at)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(device_id) DO UPDATE SET
                 lat=excluded.lat, lon=excluded.lon, speed_kmh=excluded.speed_kmh,
                 bearing=excluded.bearing, battery=excluded.battery, updated_at=excluded.updated_at""",
            (
                device_id, lat_f, lon_f, speed_kmh,
                float(p.get("bearing", 0) or 0),
                float(p["batt"]) if p.get("batt") else None,
                now,
            ),
        )

        # บันทึกจุดลง track ก็ต่อเมื่อขยับพอสมควร (กันจุดซ้อนตอนรถจอด)
        moved_m = (haversine_km(prev["lat"], prev["lon"], lat_f, lon_f) * 1000) if prev else None
        if prev is None or moved_m >= TRACK_MIN_METERS:
            conn.execute(
                "INSERT INTO tracks (device_id, lat, lon, speed_kmh, recorded_at) VALUES (?,?,?,?,?)",
                (device_id, lat_f, lon_f, speed_kmh, now),
            )
            # ล้างประวัติเก่าเกินกำหนด (เบาๆ เป็นครั้งคราว)
            conn.execute(
                "DELETE FROM tracks WHERE recorded_at < ?",
                (now - TRACK_RETENTION_HOURS * 3600,),
            )
    return PlainTextResponse("OK")


# ---------------------------------------------------- 2) ฝั่งแอดมิน (สร้าง/จบงาน)


@app.get("/admin", response_class=HTMLResponse)
def admin_page(key: str = Query("")):
    require_admin(key)
    return render("admin.html", ADMIN_KEY=key)


@app.get("/api/admin/state")
def admin_state(key: str = Query("")):
    require_admin(key)
    now = int(time.time())
    with db() as conn:
        positions = {r["device_id"]: dict(r) for r in conn.execute("SELECT * FROM positions")}
        jobs = [dict(r) for r in conn.execute(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT 30")]
    devices = []
    for did, label in DEVICES.items():
        pos = positions.get(did)
        devices.append({
            "id": did,
            "label": label,
            "online": bool(pos) and now - pos["updated_at"] <= STALE_SECONDS,
            "last_seen": pos["updated_at"] if pos else None,
            "battery": pos["battery"] if pos else None,
        })
    return {"devices": devices, "jobs": jobs, "now": now}


@app.post("/api/admin/jobs")
async def create_job(request: Request, key: str = Query("")):
    require_admin(key)
    body = await request.json()
    device_id = body.get("device_id")
    customer = (body.get("customer") or "").strip()
    if device_id not in DEVICES:
        raise HTTPException(status_code=400, detail="unknown device")
    if not customer:
        raise HTTPException(status_code=400, detail="customer required")

    token = secrets.token_urlsafe(8)
    with db() as conn:
        # ปิดงานเก่าที่ยัง active บนเครื่องเดียวกัน (1 เครื่อง = 1 งาน ณ เวลาหนึ่ง)
        conn.execute(
            "UPDATE jobs SET status='done', ended_at=? WHERE device_id=? AND status='active'",
            (int(time.time()), device_id),
        )
        conn.execute(
            """INSERT INTO jobs (token, device_id, customer, destination, dest_lat, dest_lon, created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (
                token, device_id, customer,
                (body.get("destination") or "").strip(),
                body.get("dest_lat"), body.get("dest_lon"),
                int(time.time()),
            ),
        )
    return {"token": token, "url": f"/track/{token}"}


@app.post("/api/admin/jobs/{token}/end")
def end_job(token: str, key: str = Query("")):
    require_admin(key)
    with db() as conn:
        cur = conn.execute(
            "UPDATE jobs SET status='done', ended_at=? WHERE token=? AND status='active'",
            (int(time.time()), token),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="job not found or already done")
    return {"ok": True}


# ---------------------------------------------------- 2b) ประวัติเส้นทาง (GPS track)


def _job_or_404(token: str) -> dict:
    with db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE token=?", (token,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="job not found")
    return dict(row)


@app.get("/admin/track/{token}", response_class=HTMLResponse)
def admin_track_page(token: str, key: str = Query("")):
    require_admin(key)
    job = _job_or_404(token)
    return render("route.html", TOKEN=token, ADMIN_KEY=key, CUSTOMER=job["customer"])


@app.get("/api/admin/jobs/{token}/track")
def admin_track_data(token: str, key: str = Query("")):
    require_admin(key)
    job = _job_or_404(token)
    points = job_track_points(job)
    end = job["ended_at"] or int(time.time())
    return {
        "customer": job["customer"],
        "destination": job["destination"],
        "device_id": job["device_id"],
        "status": job["status"],
        "started_at": job["created_at"],
        "ended_at": job["ended_at"],
        "duration_min": max(0, round((end - job["created_at"]) / 60)),
        "distance_km": track_length_km(points),
        "point_count": len(points),
        "dest_lat": job["dest_lat"],
        "dest_lon": job["dest_lon"],
        "points": points,
    }


@app.get("/api/admin/jobs/{token}/export")
def admin_track_export(token: str, key: str = Query(""), format: str = Query("gpx")):
    require_admin(key)
    job = _job_or_404(token)
    points = job_track_points(job)
    # ชื่อไฟล์ต้องเป็น ASCII เท่านั้น (HTTP header เข้ารหัส latin-1) — ตัวอักษรไทยจึงแทนด้วย _
    safe = "".join(c if (c.isascii() and c.isalnum()) else "_" for c in (job["customer"] or ""))[:40].strip("_")
    fname = f"{(safe or 'track')}_{token}.{ 'geojson' if format == 'geojson' else 'gpx' }"

    if format == "geojson":
        body = json.dumps({
            "type": "Feature",
            "properties": {
                "customer": job["customer"],
                "destination": job["destination"],
                "device_id": job["device_id"],
            },
            "geometry": {
                "type": "LineString",
                "coordinates": [[p["lon"], p["lat"]] for p in points],  # GeoJSON = lon,lat
            },
        }, ensure_ascii=False)
        media = "application/geo+json"
    else:
        def esc(s):
            return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
        pts = "\n".join(
            f'      <trkpt lat="{p["lat"]}" lon="{p["lon"]}">'
            f'<time>{time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(p["recorded_at"]))}</time>'
            f'<speed>{round(p["speed_kmh"] / 3.6, 2)}</speed></trkpt>'
            for p in points
        )
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<gpx version="1.1" creator="Delivery Tracker" '
            'xmlns="http://www.topografix.com/GPX/1/1">\n'
            f'  <trk><name>{esc(job["customer"])} — {esc(job["destination"] or "")}</name>\n'
            '    <trkseg>\n'
            f'{pts}\n'
            '    </trkseg>\n  </trk>\n</gpx>\n'
        )
        media = "application/gpx+xml"

    return PlainTextResponse(body, media_type=media, headers={
        "Content-Disposition": f'attachment; filename="{fname}"'
    })


# ---------------------------------------------------- 3) ฝั่งลูกค้า (แผนที่เรียลไทม์)


def get_job(token: str):
    with db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE token=?", (token,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    job = dict(row)
    # หมดอายุอัตโนมัติ
    if job["status"] == "active" and time.time() - job["created_at"] > JOB_TTL_HOURS * 3600:
        with db() as conn:
            conn.execute("UPDATE jobs SET status='done', ended_at=? WHERE token=?",
                         (int(time.time()), token))
        job["status"] = "done"
    return job


def job_track_points(job: dict):
    """ทุกจุดของ track ในช่วงเวลาของงานนี้ (device เดียวกัน ระหว่างเริ่มงาน→จบงาน/ปัจจุบัน)"""
    end = job["ended_at"] or int(time.time())
    with db() as conn:
        rows = conn.execute(
            """SELECT lat, lon, speed_kmh, recorded_at FROM tracks
               WHERE device_id=? AND recorded_at>=? AND recorded_at<=?
               ORDER BY recorded_at ASC""",
            (job["device_id"], job["created_at"], end),
        ).fetchall()
    return [dict(r) for r in rows]


def track_length_km(points):
    total = 0.0
    for a, b in zip(points, points[1:]):
        total += haversine_km(a["lat"], a["lon"], b["lat"], b["lon"])
    return round(total, 2)


@app.get("/track/{token}", response_class=HTMLResponse)
def track_page(token: str):
    job = get_job(token)   # 404 ถ้าไม่มีงานนี้
    return render("track.html", TOKEN=token, CUSTOMER=job["customer"])


@app.get("/api/track/{token}")
def track_data(token: str):
    job = get_job(token)
    if job["status"] != "active":
        return JSONResponse({"status": "done", "customer": job["customer"]})

    now = int(time.time())
    with db() as conn:
        pos = conn.execute("SELECT * FROM positions WHERE device_id=?",
                           (job["device_id"],)).fetchone()

    if not pos:
        return {"status": "waiting", "customer": job["customer"],
                "destination": job["destination"]}

    # เส้นทางที่รถวิ่งผ่านมาแล้ว (สำหรับวาด polyline บนแผนที่ลูกค้า)
    points = job_track_points(job)
    path = [[pt["lat"], pt["lon"]] for pt in points]

    data = {
        "status": "live",
        "customer": job["customer"],
        "destination": job["destination"],
        "lat": pos["lat"],
        "lon": pos["lon"],
        "speed_kmh": pos["speed_kmh"],
        "bearing": pos["bearing"],
        "seconds_ago": now - pos["updated_at"],
        "stale": now - pos["updated_at"] > STALE_SECONDS,
        "dest_lat": job["dest_lat"],
        "dest_lon": job["dest_lon"],
        "path": path,
        "traveled_km": track_length_km(points),
    }
    if job["dest_lat"] is not None and job["dest_lon"] is not None:
        km = haversine_km(pos["lat"], pos["lon"], job["dest_lat"], job["dest_lon"])
        data["distance_km"] = round(km, 1)
        # ETA คร่าวๆ จากความเร็วเฉลี่ยขั้นต่ำ 30 กม./ชม.
        data["eta_min"] = max(1, round(km / max(pos["speed_kmh"], 30) * 60))
    return data
