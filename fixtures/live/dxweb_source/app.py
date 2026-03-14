"""
AI3I DX Cluster Web Frontend
Reads spot data directly from DXSpider binary spot files.
"""
import asyncio
import json
import re
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

import ctydat

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SPIDER_DATA = Path("/home/sysop/spider/local_data")
CTY_DAT     = SPIDER_DATA / "cty.dat"
DB_PATH     = Path("/opt/dxweb/spots.db")
STATIC_DIR  = Path("/opt/dxweb/static")
POLL_SECS   = 5      # seconds between file polls
HISTORY_HRS = 24     # hours of history to pre-load on startup
DXVARS_PM   = Path("/home/sysop/spider/local/DXVars.pm")
VERSION_PM  = Path("/home/sysop/spider/perl/Version.pm")

# ---------------------------------------------------------------------------
# Band mapping  (MHz boundaries)
# ---------------------------------------------------------------------------
BANDS = [
    ("2190m",  0.1357,  0.1378),
    ("630m",   0.472,   0.479),
    ("LF/MF",  0.1357,  0.479),
    ("160m",   1.8,     2.0),
    ("80m",    3.5,     4.0),
    ("60m",    5.330,   5.407),
    ("40m",    7.0,     7.3),
    ("30m",    10.1,    10.15),
    ("20m",    14.0,    14.35),
    ("17m",    18.068,  18.168),
    ("15m",    21.0,    21.45),
    ("12m",    24.89,   24.99),
    ("10m",    28.0,    29.7),
    ("6m",     50.0,    54.0),
    ("4m",     70.0,    70.5),
    ("2m",     144.0,   148.0),
    ("1.25m",  222.0,   225.0),
    ("70cm",   430.0,   450.0),
    ("33cm",   902.0,   928.0),
    ("23cm",   1240.0,  1300.0),
]

def freq_to_band(freq_khz: float) -> str:
    mhz = freq_khz / 1000.0
    for name, lo, hi in BANDS:
        if lo <= mhz <= hi:
            return name
    if mhz > 1300.0:
        return "SHF"
    return ""

# ---------------------------------------------------------------------------
# Mode detection from comment + frequency
# ---------------------------------------------------------------------------
_MODE_RE = [
    (re.compile(r'\bFT8\b',  re.I), 'FT8'),
    (re.compile(r'\bFT4\b',  re.I), 'FT4'),
    (re.compile(r'\bFT2\b',  re.I), 'FT2'),
    (re.compile(r"\bQ65\b", re.I), "Q65"),
    (re.compile(r"\bMSK144\b", re.I), "MSK144"),
    (re.compile(r"\bFSK441\b", re.I), "FSK441"),
    (re.compile(r'\bJS8\b',  re.I), 'JS8'),
    (re.compile(r'\bJT65\b', re.I), 'JT65'),
    (re.compile(r'\bJT9\b',  re.I), 'JT9'),
    (re.compile(r'\bWSPR\b', re.I), 'WSPR'),
    (re.compile(r'\bRTTY\b', re.I), 'RTTY'),
    (re.compile(r"\bMFSK\b", re.I), "MFSK"),
    (re.compile(r"\bOLIVIA\b", re.I), "OLIVIA"),
    (re.compile(r"\bDOMINO(?:EX)?\b", re.I), "DOMINO"),
    (re.compile(r"\bTHOR\b", re.I), "THOR"),
    (re.compile(r"\bHELL(?:SCHREIBER)?\b", re.I), "HELL"),
    (re.compile(r"\bROS\b", re.I), "ROS"),
    (re.compile(r"\bVARA\b", re.I), "VARA"),
    (re.compile(r"\bPACTOR\b", re.I), "PACTOR"),
    (re.compile(r"\bWINMOR\b", re.I), "WINMOR"),
    (re.compile(r"\bARDOP\b", re.I), "ARDOP"),
    (re.compile(r'\bPSK\d*', re.I), 'PSK'),
    (re.compile(r"\bFAX\b", re.I), "FAX"),
    (re.compile(r"\bSSTV\b", re.I), "SSTV"),
    (re.compile(r"\bATV\b", re.I), "ATV"),
    (re.compile(r"\bDATA\b", re.I), "DATA"),
    (re.compile(r"\bDIGI(?:TAL)?\b", re.I), "DATA"),
    (re.compile(r'\bCW\b',   re.I), 'CW'),
    (re.compile(r'\b(LSB|USB|SSB)\b', re.I), 'SSB'),
    (re.compile(r'\bAM\b',   re.I), 'AM'),
    (re.compile(r'\bFM\b',   re.I), 'FM'),
]

# CW sub-band ranges (MHz)
_CW_RANGES = [
    (1.800, 1.840), (3.500, 3.600), (7.000, 7.040),
    (10.100, 10.150), (14.000, 14.070), (18.068, 18.100),
    (21.000, 21.070), (24.890, 24.920), (28.000, 28.070),
]

def detect_mode(comment: str, freq_khz: float) -> str:
    for rx, mode in _MODE_RE:
        if rx.search(comment):
            return mode
    mhz = freq_khz / 1000.0
    for lo, hi in _CW_RANGES:
        if lo <= mhz <= hi:
            return 'CW'
    return ''

# ---------------------------------------------------------------------------
# Activity detection
# ---------------------------------------------------------------------------
_ACT_RE = [
    (re.compile(r'\bSOTA\b', re.I), 'SOTA'),
    (re.compile(r'\bPOTA\b|\bparks?\b', re.I), 'POTA'),
    (re.compile(r'\bIOTA\b', re.I), 'IOTA'),
    (re.compile(r'\bWWFF\b', re.I), 'WWFF'),
    (re.compile(r'\bBOTA\b', re.I), 'BOTA'),
    (re.compile(r'\bLOTA\b', re.I), 'LOTA'),
    (re.compile(r'\bGMA\b', re.I), 'GMA'),
]

_MODE_ORDER = ['CW', 'WSPR', 'RTTY', 'FT8', 'FT4', 'FT2', 'JS8', 'JT9', 'JT65', 'SSB', 'AM', 'FM', 'PSK']

def detect_activity(comment: str) -> str:
    for rx, act in _ACT_RE:
        if rx.search(comment):
            return act
    return ''


def load_site_info() -> dict:
    data = {
        "mycall": "AI3I-15",
        "myalias": "AI3I",
        "mylocator": "",
        "myqth": "",
        "myemail": "",
        "version": "",
        "build": "",
    }
    if DXVARS_PM.exists():
        text = DXVARS_PM.read_text(encoding="utf-8", errors="ignore")
        for key in ("mycall", "myalias", "mylocator", "myqth", "myemail"):
            m = re.search(rf'\${key}\s*=\s*"([^"]*)"', text)
            if m:
                data[key] = m.group(1).replace("\\@", "@")
    if VERSION_PM.exists():
        text = VERSION_PM.read_text(encoding="utf-8", errors="ignore")
        for key in ("version", "build"):
            m = re.search(rf'\${key}\s*=\s*[\'"]([^\'"]+)[\'"]', text)
            if m:
                data[key] = m.group(1)
    data["cluster_name"] = f'{data["myalias"]} DXSpider'.strip()
    return data

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    with db_connect() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS spots (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                time            TEXT    NOT NULL,
                freq            REAL    NOT NULL,
                dx_call         TEXT    NOT NULL,
                spotter         TEXT    NOT NULL,
                comment         TEXT    DEFAULT '',
                band            TEXT    DEFAULT '',
                mode            TEXT    DEFAULT '',
                activity        TEXT    DEFAULT '',
                dx_entity       TEXT    DEFAULT '',
                dx_continent    TEXT    DEFAULT '',
                dx_cqz          INTEGER DEFAULT 0,
                dx_ituz         INTEGER DEFAULT 0,
                dx_lat          REAL    DEFAULT 0,
                dx_lon          REAL    DEFAULT 0,
                spotter_entity  TEXT    DEFAULT '',
                spotter_continent TEXT  DEFAULT '',
                spotter_lat     REAL    DEFAULT 0,
                spotter_lon     REAL    DEFAULT 0,
                UNIQUE(time, freq, dx_call, spotter)
            )
        ''')
        # Migrate existing databases missing dx_ituz
        try:
            conn.execute('ALTER TABLE spots ADD COLUMN dx_ituz INTEGER DEFAULT 0')
        except Exception:
            pass
        conn.execute('CREATE INDEX IF NOT EXISTS idx_time    ON spots(time)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_band    ON spots(band)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_dx_call ON spots(dx_call)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_spotter ON spots(spotter)')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS daily_stats (
                date     TEXT PRIMARY KEY,
                spots    INTEGER DEFAULT 0,
                top_band TEXT    DEFAULT '',
                by_band  TEXT    DEFAULT '{}'
            )
        ''')

def spot_to_row(spot: dict) -> tuple:
    return (
        spot['time'], spot['freq'], spot['dx_call'], spot['spotter'],
        spot['comment'], spot['band'], spot['mode'], spot['activity'],
        spot['dx_entity'], spot['dx_continent'], spot['dx_cqz'], spot['dx_ituz'],
        spot['dx_lat'], spot['dx_lon'],
        spot['spotter_entity'], spot['spotter_continent'],
        spot['spotter_lat'], spot['spotter_lon'],
    )

_INSERT_SQL = '''
    INSERT OR IGNORE INTO spots
        (time, freq, dx_call, spotter, comment, band, mode, activity,
         dx_entity, dx_continent, dx_cqz, dx_ituz, dx_lat, dx_lon,
         spotter_entity, spotter_continent, spotter_lat, spotter_lon)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

def insert_spot(conn: sqlite3.Connection, spot: dict) -> None:
    conn.execute(_INSERT_SQL, spot_to_row(spot))

def cleanup_old_spots() -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=HISTORY_HRS + 1)).isoformat()
    with db_connect() as conn:
        conn.execute('DELETE FROM spots WHERE time < ?', (cutoff,))

def snapshot_daily() -> None:
    """Accumulate today's spot stats into daily_stats."""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    start = today + 'T00:00:00+00:00'
    end   = today + 'T23:59:59.999999+00:00'
    with db_connect() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM spots WHERE time >= ? AND time <= ?", (start, end)
        ).fetchone()[0]
        bands = conn.execute(
            "SELECT band, COUNT(*) n FROM spots WHERE time >= ? AND time <= ? AND band != '' "
            "GROUP BY band ORDER BY n DESC", (start, end)
        ).fetchall()
        top_band = bands[0]['band'] if bands else ''
        by_band  = json.dumps({r['band']: r['n'] for r in bands})
        conn.execute(
            "INSERT OR REPLACE INTO daily_stats (date, spots, top_band, by_band) VALUES (?,?,?,?)",
            (today, total, top_band, by_band)
        )

# ---------------------------------------------------------------------------
# Spot file parsing
# ---------------------------------------------------------------------------
def parse_line(line: str) -> Optional[dict]:
    """
    Parse one line from a DXSpider spot data file.
    Format (fields separated by ^):
      [0] freq_khz  [1] dx_call  [2] unix_timestamp  [3] comment  [4] spotter
    """
    fields = line.strip().split('^')
    if len(fields) < 5:
        return None
    try:
        freq     = float(fields[0])
        dx_call  = fields[1].strip().upper()
        ts       = int(fields[2])
        comment  = fields[3].strip()
        spotter  = fields[4].strip().upper()
    except (ValueError, IndexError):
        return None

    if not dx_call or not spotter:
        return None

    dt   = datetime.fromtimestamp(ts, tz=timezone.utc)
    band = freq_to_band(freq)
    mode = detect_mode(comment, freq)
    act  = detect_activity(comment)

    dx_ent = ctydat.lookup(dx_call)
    sp_ent = ctydat.lookup(spotter)

    return {
        'time':              dt.isoformat(),
        'freq':              freq,
        'dx_call':           dx_call,
        'spotter':           spotter,
        'comment':           comment,
        'band':              band,
        'mode':              mode,
        'activity':          act,
        'dx_entity':         dx_ent.name      if dx_ent else '',
        'dx_continent':      dx_ent.continent if dx_ent else '',
        'dx_cqz':            dx_ent.cq_zone   if dx_ent else 0,
        'dx_ituz':           dx_ent.itu_zone  if dx_ent else 0,
        'dx_lat':            dx_ent.lat       if dx_ent else 0.0,
        'dx_lon':            dx_ent.lon       if dx_ent else 0.0,
        'spotter_entity':    sp_ent.name      if sp_ent else '',
        'spotter_continent': sp_ent.continent if sp_ent else '',
        'spotter_lat':       sp_ent.lat       if sp_ent else 0.0,
        'spotter_lon':       sp_ent.lon       if sp_ent else 0.0,
    }

def spot_file_for(dt: datetime) -> Path:
    day = dt.timetuple().tm_yday
    return SPIDER_DATA / "spots" / str(dt.year) / f"{day:03d}.dat"

# ---------------------------------------------------------------------------
# History loader
# ---------------------------------------------------------------------------
def load_history() -> None:
    """Pre-populate SQLite with the last HISTORY_HRS hours of spots."""
    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=HISTORY_HRS)

    # Collect relevant spot files (may span yesterday → today)
    files: list[Path] = []
    for delta in range(2, -1, -1):   # 2 days ago → today
        f = spot_file_for(now - timedelta(days=delta))
        if f.exists():
            files.append(f)

    with db_connect() as conn:
        for spot_file in files:
            with open(spot_file, 'r', encoding='ascii', errors='ignore') as fp:
                for raw in fp:
                    spot = parse_line(raw)
                    if spot and datetime.fromisoformat(spot['time']) >= cutoff:
                        insert_spot(conn, spot)

# ---------------------------------------------------------------------------
# Live spot polling
# ---------------------------------------------------------------------------
_file_pos: dict[str, int] = {}   # path → byte offset of last read
_ws_clients: list[WebSocket] = []

async def snapshot_loop() -> None:
    """Write daily stats snapshot every hour."""
    while True:
        await asyncio.sleep(3600)
        try:
            snapshot_daily()
        except Exception as exc:
            print(f"[snapshot_loop] {exc}")

async def poll_loop() -> None:
    """Background coroutine: tail the current spot file and push new spots."""
    while True:
        try:
            now       = datetime.now(timezone.utc)
            spot_file = spot_file_for(now)
            key       = str(spot_file)

            if spot_file.exists():
                size = spot_file.stat().st_size
                last = _file_pos.get(key)

                if last is None:
                    # First visit: start from end so we don't replay history
                    _file_pos[key] = size
                elif size > last:
                    new_spots: list[dict] = []
                    with open(spot_file, 'r', encoding='ascii', errors='ignore') as fp:
                        fp.seek(last)
                        for raw in fp:
                            spot = parse_line(raw)
                            if spot:
                                new_spots.append(spot)
                        _file_pos[key] = fp.tell()

                    if new_spots:
                        with db_connect() as conn:
                            for spot in new_spots:
                                insert_spot(conn, spot)

                        if _ws_clients:
                            payload = json.dumps(new_spots)
                            dead: list[WebSocket] = []
                            for ws in list(_ws_clients):
                                try:
                                    await ws.send_text(payload)
                                except Exception:
                                    dead.append(ws)
                            for ws in dead:
                                _ws_clients.remove(ws)

                elif size < last:
                    # File was truncated / rotated
                    _file_pos[key] = 0

        except Exception as exc:
            print(f"[poll_loop] {exc}")

        await asyncio.sleep(POLL_SECS)

# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    ctydat.load_cty(str(CTY_DAT))
    init_db()
    cleanup_old_spots()
    load_history()
    snapshot_daily()   # seed today's row immediately on startup
    asyncio.create_task(poll_loop())
    asyncio.create_task(snapshot_loop())
    yield

app = FastAPI(title="AI3I DX Cluster", lifespan=lifespan)

# --- REST endpoints ---------------------------------------------------------

@app.get("/api/spots")
async def api_spots(
    band:     str = "",
    mode:     str = "",
    activity: str = "",
    search:   str = "",
    limit:    int = 200,
):
    with db_connect() as conn:
        clauses = ["1=1"]
        params:  list = []
        if band     and band     != "ALL": clauses.append("band = ?");     params.append(band)
        if mode     and mode     != "ALL": clauses.append("mode = ?");     params.append(mode)
        if activity and activity != "ALL": clauses.append("activity = ?"); params.append(activity)
        if search:
            clauses.append("(dx_call LIKE ? OR spotter LIKE ? OR comment LIKE ?)")
            params += [f"%{search}%"] * 3

        sql  = f"SELECT * FROM spots WHERE {' AND '.join(clauses)} ORDER BY time DESC LIMIT ?"
        params.append(min(limit, 500))
        rows = conn.execute(sql, params).fetchall()

    return JSONResponse([dict(r) for r in rows])


@app.get("/api/stats")
async def api_stats():
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    with db_connect() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM spots WHERE time > ?", (cutoff,)
        ).fetchone()[0]
        bands = conn.execute(
            "SELECT band, COUNT(*) n FROM spots WHERE time > ? AND band != '' "
            "GROUP BY band ORDER BY n DESC", (cutoff,)
        ).fetchall()
        modes = conn.execute(
            "SELECT mode, COUNT(*) n FROM spots WHERE time > ? AND mode != '' "
            "GROUP BY mode ORDER BY n DESC", (cutoff,)
        ).fetchall()
    mode_rank = {mode: idx for idx, mode in enumerate(_MODE_ORDER)}
    modes = sorted(modes, key=lambda r: (mode_rank.get(r["mode"], len(_MODE_ORDER)), -r["n"], r["mode"]))

    return JSONResponse({
        "total": total,
        "bands": [{"band": r["band"], "count": r["n"]} for r in bands],
        "modes": [{"mode": r["mode"], "count": r["n"]} for r in modes],
    })


# --- Leaderboard ------------------------------------------------------------

@app.get("/api/leaderboard")
async def api_leaderboard(hours: int = 24):
    hours = max(1, min(hours, 24))
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with db_connect() as conn:
        spotters = conn.execute(
            "SELECT spotter, COUNT(*) n FROM spots WHERE time > ? "
            "GROUP BY spotter ORDER BY n DESC LIMIT 15", (cutoff,)
        ).fetchall()
        dx = conn.execute(
            "SELECT dx_call, dx_entity, COUNT(*) n FROM spots WHERE time > ? "
            "GROUP BY dx_call ORDER BY n DESC LIMIT 15", (cutoff,)
        ).fetchall()
        entities = conn.execute(
            "SELECT dx_entity, COUNT(*) n FROM spots WHERE time > ? AND dx_entity != '' "
            "GROUP BY dx_entity ORDER BY n DESC LIMIT 25", (cutoff,)
        ).fetchall()
        continents = conn.execute(
            "SELECT dx_continent, COUNT(*) n FROM spots WHERE time > ? AND dx_continent != '' "
            "GROUP BY dx_continent ORDER BY n DESC", (cutoff,)
        ).fetchall()
        bands = conn.execute(
            "SELECT band, COUNT(*) n FROM spots WHERE time > ? AND band != '' "
            "GROUP BY band ORDER BY n DESC", (cutoff,)
        ).fetchall()
        by_hour = conn.execute(
            "SELECT strftime('%H', time) hr, COUNT(*) n FROM spots WHERE time > ? "
            "GROUP BY hr ORDER BY hr", (cutoff,)
        ).fetchall()
        band_hour = conn.execute(
            "SELECT band, CAST(strftime('%H', time) AS INTEGER) hr, COUNT(*) n "
            "FROM spots WHERE time > ? AND band != '' "
            "GROUP BY band, hr", (cutoff,)
        ).fetchall()
    return JSONResponse({
        "spotters":   [{"call": r["spotter"], "count": r["n"]} for r in spotters],
        "dx":         [{"call": r["dx_call"], "entity": r["dx_entity"], "count": r["n"]} for r in dx],
        "entities":   [{"entity": r["dx_entity"], "count": r["n"]} for r in entities],
        "continents": [{"cont": r["dx_continent"], "count": r["n"]} for r in continents],
        "bands":      [{"band": r["band"], "count": r["n"]} for r in bands],
        "by_hour":    [{"hour": int(r["hr"]), "count": r["n"]} for r in by_hour],
        "band_hour":  [{"band": r["band"], "hour": r["hr"], "count": r["n"]} for r in band_hour],
    })


# --- History ----------------------------------------------------------------

@app.get("/api/history")
async def api_history():
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT date, spots, top_band, by_band FROM daily_stats ORDER BY date DESC LIMIT 14"
        ).fetchall()
    return JSONResponse([{
        'date':     r['date'],
        'spots':    r['spots'],
        'top_band': r['top_band'],
        'by_band':  json.loads(r['by_band']),
    } for r in rows])


# --- Cluster node map -------------------------------------------------------

_node_cache: dict = {"ts": 0.0, "data": []}
_NODE_STR_RE = re.compile(r"(\w+) => '([^']*)'")
_NODE_NUM_RE = re.compile(r"(\w+) => ([\d.-]+)(?=[,\s}])")

def parse_node_file() -> list:
    """Extract cluster node records (sort=S and sort=A) from user_asc."""
    path = SPIDER_DATA / "user_asc"
    if not path.exists():
        return []
    result = []
    with open(path, 'r', encoding='ascii', errors='ignore') as f:
        for line in f:
            if "sort => 'S'" not in line and "sort => 'A'" not in line:
                continue
            fields: dict = {}
            for m in _NODE_STR_RE.finditer(line):
                fields[m.group(1)] = m.group(2)
            for m in _NODE_NUM_RE.finditer(line):
                if m.group(1) not in fields:
                    fields[m.group(1)] = m.group(2)
            call = fields.get('call', '')
            if not call:
                continue
            sort_val  = fields.get('sort', '')
            node_type = 'peer' if sort_val == 'S' else 'node'
            try:
                lat = float(fields['lat'])
                lon = float(fields['long'])
            except (KeyError, ValueError):
                base = re.sub(r'-\d+$', '', call)
                ent  = ctydat.lookup(base)
                if ent:
                    lat, lon = ent.lat, ent.lon
                else:
                    continue
            try:
                lastin = int(float(fields.get('lastin', 0) or 0))
            except (ValueError, TypeError):
                lastin = 0
            result.append({
                'call':    call,
                'sort':    node_type,
                'lat':     lat,
                'lon':     lon,
                'lastin':  lastin,
                'qra':     fields.get('qra', ''),
                'qth':     fields.get('qth', ''),
                'name':    fields.get('name', ''),
            })
    return result

@app.get("/api/nodes")
async def api_nodes():
    now_ts = datetime.now(timezone.utc).timestamp()
    if _node_cache["ts"] and now_ts - _node_cache["ts"] < 300:
        return JSONResponse(_node_cache["data"])
    data = parse_node_file()
    _node_cache["ts"]   = now_ts
    _node_cache["data"] = data
    return JSONResponse(data)


# --- Live cluster network (from today's PC92 log) ---------------------------

_net_cache: dict = {"ts": 0.0, "data": {}}
_PC92_RE = re.compile(r'PC92A\s+(\S+)\s+->\s+\S+\s+on\s+(\S+)')
_SSID_RE = re.compile(r'^[A-Z0-9]+(?:/[A-Z0-9]+)?-\d+$')

def build_network() -> dict:
    """Parse today's DXSpider log for live node topology via PC92A packets."""
    now = datetime.now(timezone.utc)
    log_path = SPIDER_DATA / "log" / str(now.year) / f"{now.month:02d}.dat"

    # Build lat/lon lookup from user_asc (call -> {lat,lon,qth,qra})
    node_info: dict = {}
    for n in parse_node_file():
        node_info[n['call']] = n

    def latlon(call: str):
        if call in node_info:
            return node_info[call]['lat'], node_info[call]['lon']
        base = re.sub(r'-\d+$', '', call)
        try:
            ent = ctydat.lookup(base)
            if ent:
                return ent.lat, ent.lon
        except Exception:
            pass
        return None, None

    active: dict = {}   # call -> {lat, lon, qth, qra, links:[]}
    links:  set  = set()

    # First pass: collect all calls that appear as 'dst' — these are cluster nodes
    cluster_nodes: set = set()
    raw_links: list = []
    if log_path.exists():
        with open(log_path, 'r', encoding='ascii', errors='ignore') as f:
            for line in f:
                m = _PC92_RE.search(line)
                if not m:
                    continue
                src, dst = m.group(1), m.group(2)
                cluster_nodes.add(dst)
                raw_links.append((src, dst))

    # Second pass: only record src→dst links where src is also a known cluster node
    for src, dst in raw_links:
        if src in cluster_nodes:
            link = tuple(sorted([src, dst]))
            links.add(link)

    for call in cluster_nodes:
        if call not in active:
            base = re.sub(r'-\d+$', '', call)
            try:
                ent = ctydat.lookup(base)
            except Exception:
                ent = None
            if ent is None:
                continue
            active[call] = {
                'call':   call,
                'entity': ent.name,
                'lat':    ent.lat,
                'lon':    ent.lon,
            }

    return {
        'nodes': list(active.values()),
        'links': [list(l) for l in links],
        'home':  'AI3I-15',
    }

@app.get("/api/network")
async def api_network():
    now_ts = datetime.now(timezone.utc).timestamp()
    if _net_cache["ts"] and now_ts - _net_cache["ts"] < 120:
        return JSONResponse(_net_cache["data"])
    data = build_network()
    _net_cache["ts"]   = now_ts
    _net_cache["data"] = data
    return JSONResponse(data)


# --- Solar proxy ------------------------------------------------------------

_solar_cache: dict = {"ts": 0.0, "data": {}}

@app.get("/api/solar")
async def api_solar():
    import urllib.request
    import xml.etree.ElementTree as ET
    now_ts = datetime.now(timezone.utc).timestamp()
    if _solar_cache["ts"] and now_ts - _solar_cache["ts"] < 600:
        return JSONResponse(_solar_cache["data"])
    try:
        req = urllib.request.Request(
            "https://www.hamqsl.com/solarxml.php",
            headers={"User-Agent": "AI3I-DXCluster/1.0 (+https://dxcluster.ai3i.net)"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            xml_bytes = r.read()
        root = ET.fromstring(xml_bytes)
        sd   = root.find("solardata")

        def g(tag: str) -> str:
            el = sd.find(tag) if sd is not None else None
            return el.text.strip() if (el is not None and el.text) else ""

        cond: dict = {}
        if sd is not None:
            for b in sd.findall("calculatedconditions/band"):
                key = f"{b.get('name', '')}_{b.get('time', '')}"
                cond[key] = b.text.strip() if b.text else ""

        vhf: list = []
        if sd is not None:
            for ph in sd.findall("calculatedvhfconditions/phenomenon"):
                vhf.append({
                    "name":      ph.get("name", ""),
                    "location":  ph.get("location", ""),
                    "condition": ph.text.strip() if ph.text else "",
                })

        result = {
            "sfi": g("solarflux"), "sn": g("sunspots"),
            "a":   g("aindex"),    "k":  g("kindex"),
            "xray": g("xray"),     "solarwind": g("solarwind"),
            "aurora": g("aurora"), "updated": g("updated"),
            "conditions": cond,    "vhf": vhf,
        }
        _solar_cache["ts"]   = now_ts
        _solar_cache["data"] = result
        return JSONResponse(result)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=503)


@app.get("/api/site-info")
async def api_site_info():
    return JSONResponse(load_site_info())


# --- WebSocket --------------------------------------------------------------

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.append(ws)
    try:
        while True:
            await ws.receive_text()   # keepalive — browser sends pings
    except WebSocketDisconnect:
        pass
    finally:
        if ws in _ws_clients:
            _ws_clients.remove(ws)


# --- Static files (must be last) -------------------------------------------
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8080, reload=False, log_level="info")
