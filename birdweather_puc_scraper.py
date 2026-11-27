#!/usr/bin/env python3
"""
Ruft von BirdWeather-PUC-Stationen Daten für Zeiträume oder Einzeltage ab:
  - Standort (stations.csv)
  - Detections (detections.csv)
  - Sensordaten kombiniert pro Request (sensor_<typ>.csv)
  - Aktuelle OpenWeather-Quellen-Koordinaten (air_pollution_coords.csv)
"""
import argparse
import csv
import os
import shutil
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

URL = "https://app.birdweather.com/graphql"
PAGE_SIZE = 500

NE = {"lat": 55.1, "lon": 15.1}
SW = {"lat": 47.2, "lon": 5.8}
GERMANY_NAMES = {"germany", "deutschland", "de"}
DEFAULT_TZ = "Europe/Berlin"

_local = threading.local()
_page_cap_reported = False


class AdaptivePacer:
    """Passt die Wartezeit zwischen Requests automatisch an das Server-Limit an."""
    def __init__(self, initial=0.5, min_pause=0.15, max_pause=5.0):
        self.lock = threading.Lock()
        self.pause = initial
        self.min_pause = min_pause
        self.max_pause = max_pause

    def get(self):
        with self.lock:
            return self.pause

    def on_success(self):
        with self.lock:
            self.pause = max(self.min_pause, self.pause - 0.005)

    def on_rate_limit(self):
        with self.lock:
            self.pause = min(self.max_pause, self.pause + 0.25)
            return self.pause


pacer = AdaptivePacer(initial=0.5)


def get_session():
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    return _local.session


def check_server_page_cap(conn):
    """Informiert einmalig, falls der Server PAGE_SIZE stillschweigend begrenzt."""
    global _page_cap_reported
    if _page_cap_reported or not conn:
        return
    nodes = conn.get("nodes") or []
    has_next = (conn.get("pageInfo") or {}).get("hasNextPage", False)
    if has_next and 0 < len(nodes) < PAGE_SIZE:
        _page_cap_reported = True
        print(f"    [Info] Server begrenzt Seitengröße auf max. {len(nodes)} Einträge pro Request.")


def gql(query, variables, retries=10, allow_partial=False, stop_event=None):
    """POST an die API mit dynamischem Backoff und adaptiver Tempo-Anpassung."""
    base_pause = 2.0
    for attempt in range(retries):
        if stop_event and stop_event.is_set():
            return None
        try:
            r = get_session().post(URL, json={"query": query, "variables": variables}, timeout=90)

            if r.status_code == 429:
                new_pause = pacer.on_rate_limit()
                retry_after = r.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_time = int(retry_after) + 1
                else:
                    wait_time = base_pause * (2 ** attempt)

                print(f"    [Rate-Limit 429] Warte {wait_time}s (neue Request-Pause: {new_pause:.2f}s)...")
                time.sleep(wait_time)
                continue

            r.raise_for_status()
            body = r.json()
        except (requests.RequestException, ValueError):
            time.sleep(base_pause * (2 ** attempt))
            continue

        if body.get("errors") and not (allow_partial and body.get("data")):
            raise RuntimeError(body["errors"])

        pacer.on_success()
        return body["data"]

    raise RuntimeError("API dauerhaft blockiert. Später erneut versuchen.")


def paginate(query, variables, get_connection, allow_partial=False, stop_event=None):
    after = None
    while True:
        if stop_event and stop_event.is_set():
            return
        data = gql(query, {**variables, "first": PAGE_SIZE, "after": after},
                   allow_partial=allow_partial, stop_event=stop_event)
        if data is None:
            return
        conn = get_connection(data)
        if conn is None:
            return
        check_server_page_cap(conn)
        yield from conn["nodes"] or []
        if not conn["pageInfo"]["hasNextPage"]:
            return
        after = conn["pageInfo"]["endCursor"]
        time.sleep(pacer.get())


STATIONS_Q = """
query($first: Int, $after: String, $ne: InputLocation, $sw: InputLocation) {
  stations(first: $first, after: $after, ne: $ne, sw: $sw) {
    nodes {
      id name type country state location locationPrivacy
      earliestDetectionAt latestDetectionAt
      coords { lat lon }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

STATION_BY_ID_Q = """
query($id: ID!) {
  station(id: $id) {
    id name type country state location locationPrivacy timezone
    earliestDetectionAt latestDetectionAt
    coords { lat lon }
  }
}
"""

TIMEZONES_Q = """
query($first: Int, $after: String, $ne: InputLocation, $sw: InputLocation) {
  stations(first: $first, after: $after, ne: $ne, sw: $sw) {
    nodes { id timezone }
    pageInfo { hasNextPage endCursor }
  }
}
"""
#soundscape entfernt, da vermutlich unnötiger Bloat
DETECTIONS_Q = """
query($ids: [ID!], $period: InputDuration, $first: Int, $after: String) {
  detections(stationIds: $ids, period: $period, first: $first, after: $after) {
    nodes {
      id timestamp confidence probability score certainty
      coords { lat lon }
      species { id commonName scientificName }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

AIR_POLLUTION_COORDS_Q = """
query($id: ID!) {
  station(id: $id) {
    airPollution {
      timestamp
      coords { lat lon }
    }
  }
}
"""

SENSOR_FIELDS = {
    "environment": ["temperature", "humidity", "barometricPressure", "voc",
                    "eco2", "aqi", "soundPressureLevel"],
    "light": ["clear", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "nir"],
    "mag": ["x", "y", "z"],
    "accel": ["x", "y", "z"],
    "system": ["batteryVoltage", "firmware", "powerSource", "sdAvailable",
               "sdCapacity", "uploadingCompleted", "uploadingTotal", "wifiRssi"],
    "location": ["lat", "lon", "altitude", "satellites"],
}

AIR_POLLUTION_COORDS_COLS = ["station_id", "date", "timestamp", "lat", "lon"]


def combined_sensor_query(kinds):
    """Erstellt eine einzelne GraphQL-Query für mehrere Sensoren gleichzeitig."""
    var_defs = ", ".join(["$id: ID!", "$period: InputDuration", "$first: Int"]
                         + [f"$after_{k}: String" for k in kinds])
    blocks = []
    for k in kinds:
        fields = " ".join(["timestamp"] + SENSOR_FIELDS[k])
        blocks.append(f"""
      {k}History(period: $period, first: $first, after: $after_{k}) {{
        nodes {{ {fields} }}
        pageInfo {{ hasNextPage endCursor }}
      }}""")
    return f"""
query({var_defs}) {{
  station(id: $id) {{
    sensors {{{"".join(blocks)}
    }}
  }}
}}
"""


_tzfinder = None


def tz_from_coords(lat, lon):
    global _tzfinder
    if lat is None or lon is None:
        return None
    try:
        if _tzfinder is None:
            from timezonefinder import TimezoneFinder
            _tzfinder = TimezoneFinder()
        return _tzfinder.timezone_at(lat=lat, lng=lon)
    except Exception:
        return None


def station_tz(station):
    for name in (station.get("timezone"), DEFAULT_TZ):
        try:
            if name:
                return ZoneInfo(name)
        except Exception:
            pass
    return ZoneInfo("UTC")


def station_period(tz, start_date, end_date):
    start = datetime.combine(start_date, dtime.min, tzinfo=tz).astimezone(timezone.utc)
    end = datetime.combine(end_date + timedelta(days=1), dtime.min, tzinfo=tz).astimezone(timezone.utc)
    return {"from": start.date().isoformat(),
            "to": (end.date() + timedelta(days=1)).isoformat()}


def to_local_date(ts, tz):
    if not ts:
        return None
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(tz).date()


def in_date_range(ts, tz, start_date, end_date):
    d = to_local_date(ts, tz)
    if d is None:
        return False
    return start_date <= d <= end_date


def is_active_in_range(station, tz, start_date, end_date):
    latest = to_local_date(station.get("latestDetectionAt"), tz)
    earliest = to_local_date(station.get("earliestDetectionAt"), tz)
    if latest is None or latest < start_date:
        return False
    if earliest is not None and earliest > end_date:
        return False
    return True


STATION_COLS = ["id", "name", "type", "country", "state", "location", "timezone",
                "lat", "lon", "locationPrivacy", "earliestDetectionAt",
                "latestDetectionAt", "timezone_source", "status"]
DETECTION_COLS = ["station_id", "id", "timestamp", "confidence", "probability", "score",
                  "certainty", "species_id", "commonName", "scientificName", "lat", "lon",
                  "soundscape_url", "soundscape_start", "soundscape_end",
                  "soundscape_duration", "soundscape_mode"]


def sensor_cols(kind):
    return ["station_id", "timestamp"] + SENSOR_FIELDS[kind]


def flat_station(s, status):
    c = s.get("coords") or {}
    row = {k: s.get(k) for k in STATION_COLS}
    row.update(lat=c.get("lat"), lon=c.get("lon"), status=status)
    return row


def flat_detection(d, station_id):
    sp, ss, c = d.get("species") or {}, d.get("soundscape") or {}, d.get("coords") or {}
    return {
        "station_id": station_id,
        "id": d["id"], "timestamp": d["timestamp"], "confidence": d.get("confidence"),
        "probability": d.get("probability"), "score": d.get("score"),
        "certainty": d.get("certainty"),
        "species_id": sp.get("id"), "commonName": sp.get("commonName"),
        "scientificName": sp.get("scientificName"),
        "lat": c.get("lat"), "lon": c.get("lon"),
        "soundscape_url": ss.get("url"), "soundscape_start": ss.get("startTime"),
        "soundscape_end": ss.get("endTime"), "soundscape_duration": ss.get("duration"),
        "soundscape_mode": ss.get("mode"),
    }


def write_csv_atomic(path, columns, rows):
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)


def merge_parts(parts_dir, prefix, station_ids, out_path, columns):
    n = 0
    tmp = out_path.with_suffix(".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for sid in station_ids:
            p = parts_dir / f"{prefix}_{sid}.csv"
            if not p.exists():
                continue
            with open(p, newline="", encoding="utf-8") as pf:
                for row in csv.DictReader(pf):
                    w.writerow(row)
                    n += 1
    os.replace(tmp, out_path)
    return n


def fetch_detections(sid, tz, start_date, end_date, period, stop_event=None):
    return [flat_detection(d, sid)
            for d in paginate(DETECTIONS_Q, {"ids": [sid], "period": period},
                              lambda data: data["detections"], stop_event=stop_event)
            if d and in_date_range(d["timestamp"], tz, start_date, end_date)]


def fetch_sensors_combined(kinds, sid, tz, start_date, end_date, period, stop_event=None):
    """Fragt mehrere Sensoren gleichzeitig pro Seitenaufruf ab."""
    cursors = {k: None for k in kinds}
    active = {k: True for k in kinds}
    results = {k: [] for k in kinds}

    while any(active.values()):
        if stop_event and stop_event.is_set():
            return None
        active_kinds = [k for k, is_act in active.items() if is_act]
        query = combined_sensor_query(active_kinds)
        variables = {
            "id": sid,
            "period": period,
            "first": PAGE_SIZE,
            **{f"after_{k}": cursors[k] for k in active_kinds},
        }
        data = gql(query, variables, stop_event=stop_event)
        if data is None:
            return None

        sensors_data = (data.get("station") or {}).get("sensors") or {}
        for k in active_kinds:
            conn = sensors_data.get(f"{k}History")
            if not conn:
                active[k] = False
                continue
            check_server_page_cap(conn)
            for r in conn.get("nodes") or []:
                if r and in_date_range(r["timestamp"], tz, start_date, end_date):
                    results[k].append({"station_id": sid, **r})
            page_info = conn.get("pageInfo") or {}
            if page_info.get("hasNextPage"):
                cursors[k] = page_info.get("endCursor")
            else:
                active[k] = False

        if any(active.values()):
            time.sleep(pacer.get())

    return results


def fetch_air_pollution_coords(sid, stop_event=None):
    """Holt die aktuellen OpenWeather-Koordinaten der Station ab."""
    try:
        data = gql(AIR_POLLUTION_COORDS_Q, {"id": sid}, stop_event=stop_event)
        if data is None:
            return []
        ap = (data.get("station") or {}).get("airPollution")
        if ap:
            c = ap.get("coords") or {}
            return [{
                "station_id": sid,
                "date": "current",
                "timestamp": ap.get("timestamp"),
                "lat": c.get("lat"),
                "lon": c.get("lon"),
            }]
    except Exception as e:
        print(f"    [AirPollution-Coords FEHLER]: {e}")
    return []


def process_station(s, ctx):
    args, start_date, end_date, parts = ctx["args"], ctx["start_date"], ctx["end_date"], ctx["parts"]
    stop_event = ctx["stop"]
    sid, tz = s["id"], station_tz(s)
    period = station_period(tz, start_date, end_date)

    selected_sensors = args.sensors if args.sensors is not None else []
    total_parts = (0 if args.no_detections else 1) + len(selected_sensors) + (
        1 if getattr(args, "check_air_pollution_coords", False) else 0
    )

    res = {"new": 0, "skipped": 0, "failed": []}
    lines = []

    # 1) Detections
    if not args.no_detections:
        det_path = parts / f"det_{sid}.csv"
        if det_path.exists():
            res["skipped"] += 1
        elif not stop_event.is_set():
            try:
                rows = fetch_detections(sid, tz, start_date, end_date, period, stop_event=stop_event)
                if not stop_event.is_set():
                    write_csv_atomic(det_path, DETECTION_COLS, rows)
                    res["new"] += 1
                    if rows:
                        lines.append(f"    det: {len(rows)}")
            except Exception as e:
                lines.append(f"    det FEHLER: {e}")
                res["failed"].append(f"{sid}:det")

    # 2) Alle noch fehlenden Sensoren kombiniert abrufen
    todo_sensors = []
    for kind in selected_sensors:
        if (parts / f"{kind}_{sid}.csv").exists():
            res["skipped"] += 1
        else:
            todo_sensors.append(kind)

    if todo_sensors and not stop_event.is_set():
        try:
            sensor_results = fetch_sensors_combined(
                todo_sensors, sid, tz, start_date, end_date, period, stop_event=stop_event
            )
            if sensor_results is not None and not stop_event.is_set():
                for kind in todo_sensors:
                    rows = sensor_results[kind]
                    write_csv_atomic(parts / f"{kind}_{sid}.csv", sensor_cols(kind), rows)
                    res["new"] += 1
                    if rows:
                        lines.append(f"    {kind}: {len(rows)}")
        except Exception as e:
            label = "+".join(todo_sensors)
            lines.append(f"    {label} FEHLER: {e}")
            for kind in todo_sensors:
                res["failed"].append(f"{sid}:{kind}")

    # 3) Optional: AirPollution Coords
    if getattr(args, "check_air_pollution_coords", False):
        ap_path = parts / f"air_pollution_coords_{sid}.csv"
        if ap_path.exists():
            res["skipped"] += 1
        elif not stop_event.is_set():
            try:
                rows = fetch_air_pollution_coords(sid, stop_event=stop_event)
                if not stop_event.is_set():
                    write_csv_atomic(ap_path, AIR_POLLUTION_COORDS_COLS, rows)
                    res["new"] += 1
                    if rows:
                        lines.append(f"    air_pollution_coords: {len(rows)}")
            except Exception as e:
                lines.append(f"    air_pollution_coords FEHLER: {e}")
                res["failed"].append(f"{sid}:air_pollution_coords")

    with ctx["lock"]:
        ctx["done"] += 1
        if res["new"] == 0 and not res["failed"] and res["skipped"] == total_parts:
            status = "bereits komplett"
        else:
            status = f"{res['new']}/{total_parts} Teile abgerufen"
        elapsed = (time.time() - ctx["t0"]) / 60
        print(f"[{ctx['done']}/{ctx['total']}] Station {sid} {s.get('name')!r}: {status}"
              f"  ({elapsed:.1f} min)")
        for line in lines:
            print(line)
    return res


def main():
    global DEFAULT_TZ
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default="2026-10-22", help="Startdatum (YYYY-MM-DD), Stationszeit")
    ap.add_argument("--end-date", default=None, help="Enddatum (YYYY-MM-DD), Stationszeit. Optional.")
    ap.add_argument("--out", default="birdweather_out", help="Ausgabeordner")
    ap.add_argument("--world", action="store_true", help="Alle PUC-Stationen weltweit statt nur Deutschland")
    ap.add_argument("--stations", nargs="+", help="Spezifische Stations-IDs abrufen (z.B. --stations 123 456).")
    ap.add_argument("--sensors", nargs="*", default=list(SENSOR_FIELDS),
                    choices=list(SENSOR_FIELDS), help="Welche Sensortypen abgerufen werden")
    ap.add_argument("--check-air-pollution-coords", action="store_true",
                    help="Prüft die Quelle der Air-Pollution-Daten (1 Stichprobe pro Station).")
    ap.add_argument("--no-country-filter", action="store_true",
                    help="Deutschland-Modus: nur Bounding Box verwenden, nicht nach Land filtern")
    ap.add_argument("--only-active", action="store_true",
                    help="Stationen auslassen, die am Tag laut Detection-Zeitpunkten inaktiv waren")
    ap.add_argument("--no-detections", action="store_true",
                    help="Keine Detections abrufen (nur Standort und Sensordaten)")
    ap.add_argument("--workers", type=int, default=1,
                    help="Anzahl paralleler Stationsabrufe (Standard 1)")
    ap.add_argument("--fresh", action="store_true",
                    help="Vorhandene Teildateien dieses Zeitraums löschen und komplett neu abrufen")
    args = ap.parse_args()

    if args.world or args.stations:
        DEFAULT_TZ = "UTC"

    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date) if args.end_date else start_date

    if end_date < start_date:
        sys.exit("Fehler: Enddatum darf nicht vor dem Startdatum liegen.")

    folder_name = start_date.isoformat() if start_date == end_date else f"{start_date.isoformat()}_{end_date.isoformat()}"
    out = Path(args.out) / folder_name
    parts = out / "parts"

    if args.fresh and parts.exists():
        shutil.rmtree(parts)
    parts.mkdir(parents=True, exist_ok=True)

    # 1) PUC-Stationen laden
    pucs = []
    if args.stations:
        print(f"Lade {len(args.stations)} spezifische Station(en) direkt über ihre ID...")
        for sid in args.stations:
            try:
                data = gql(STATION_BY_ID_Q, {"id": sid})
                st = data.get("station") if data else None
                if st:
                    pucs.append(st)
                else:
                    print(f"  Warnung: Station {sid} nicht gefunden.")
            except Exception as e:
                print(f"  Fehler beim Abruf von Station {sid}: {e}")
    else:
        print(f"Lade Stationen ({'weltweit' if args.world else 'Deutschland'}) ...")
        variables = {} if args.world else {"ne": NE, "sw": SW}
        raw = list(paginate(STATIONS_Q, variables, lambda d: d["stations"], allow_partial=True))
        stations = [s for s in raw if s]
        print(f"  {len(stations)} Stationen geladen")

        pucs = [s for s in stations if s["type"] == "puc"]
        countries = Counter(s.get("country") for s in pucs)
        print("  Länder der PUCs:", dict(countries.most_common(15)),
              "..." if len(countries) > 15 else "")

        if not args.world and not args.no_country_filter:
            pucs = [s for s in pucs if (s.get("country") or "").lower() in GERMANY_NAMES]

        print(f"  {len(pucs)} PUC-Stationen nach Filter")

        if pucs:
            print("Lade Zeitzonen ...")
            tzmap = {s["id"]: s["timezone"]
                     for s in paginate(TIMEZONES_Q, variables, lambda d: d["stations"], allow_partial=True)
                     if s and s.get("timezone")}
            for s in pucs:
                if s["id"] in tzmap:
                    s["timezone"] = tzmap[s["id"]]

    if not pucs:
        sys.exit("Keine gültigen PUC-Stationen gefunden.")

    for s in pucs:
        if s.get("timezone"):
            s["timezone_source"] = "api"
        else:
            c = s.get("coords") or {}
            name = tz_from_coords(c.get("lat"), c.get("lon"))
            if name:
                s["timezone"], s["timezone_source"] = name, "coords"
            else:
                s["timezone"], s["timezone_source"] = None, "default"

    print(f"  {len(pucs)} Station(en) bereit zur Abfrage.")

    # 2) Optional: inaktive Stationen aussortieren
    to_query, station_rows = [], []
    for s in pucs:
        if args.only_active and not is_active_in_range(s, station_tz(s), start_date, end_date):
            station_rows.append(flat_station(s, "skipped_inactive"))
        else:
            to_query.append(s)
            station_rows.append(flat_station(s, "queried"))
    if args.only_active:
        print(f"  {len(pucs) - len(to_query)} inaktive Stationen übersprungen, "
              f"{len(to_query)} werden abgefragt")

    write_csv_atomic(out / "stations.csv", STATION_COLS, station_rows)

    # 3) Pro Station: Detections + Sensordaten
    ctx = {"args": args, "start_date": start_date, "end_date": end_date, "parts": parts,
           "stop": threading.Event(), "lock": threading.Lock(),
           "done": 0, "total": len(to_query), "t0": time.time()}
    interrupted = False
    t0 = time.time()

    ex = ThreadPoolExecutor(max_workers=max(1, args.workers))
    futures = [ex.submit(process_station, s, ctx) for s in to_query]
    try:
        for _ in as_completed(futures):
            pass
        ex.shutdown()
    except KeyboardInterrupt:
        interrupted = True
        ctx["stop"].set()
        print("\nAbbruch: warte auf laufenden Request, bereits gespeicherte Teile bleiben erhalten ...")
        ex.shutdown(wait=True, cancel_futures=True)

    failed, newly_fetched, already_done = [], 0, 0
    for f in futures:
        if f.done() and not f.cancelled():
            try:
                r = f.result()
            except Exception as e:
                failed.append(f"unerwartet: {e}")
                continue
            newly_fetched += r["new"]
            already_done += r["skipped"]
            failed += r["failed"]

    # 4) Fertige CSV-Dateien zusammensetzen
    ids = [s["id"] for s in to_query]
    totals = {}
    if not args.no_detections:
        totals["detections"] = merge_parts(parts, "det", ids, out / "detections.csv", DETECTION_COLS)

    selected_sensors = args.sensors if args.sensors is not None else []
    for kind in selected_sensors:
        totals[kind] = merge_parts(parts, kind, ids, out / f"sensor_{kind}.csv", sensor_cols(kind))

    if getattr(args, "check_air_pollution_coords", False):
        totals["air_pollution_coords"] = merge_parts(
            parts, "air_pollution_coords", ids, out / "air_pollution_coords.csv", AIR_POLLUTION_COORDS_COLS
        )

    print(f"\n{'Zwischenstand' if interrupted else 'Fertig'} nach {(time.time() - t0) / 60:.1f} min.")
    print(f"Teile neu abgerufen: {newly_fetched}, bereits vorhanden: {already_done}")
    print("Zeilen insgesamt:", totals)
    if failed:
        print(f"{len(failed)} Teile fehlgeschlagen. Skript erneut starten, um sie nachzuholen.")
    if interrupted:
        print("Zum Fortsetzen denselben Befehl noch einmal ausführen.")
    print(f"Dateien in: {out.resolve()}")
    if interrupted:
        sys.exit(130)


if __name__ == "__main__":
    main()