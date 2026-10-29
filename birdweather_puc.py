#!/usr/bin/env python3
"""
Ruft von BirdWeather-PUC-Stationen für einen Tag ab:
  - Standort (stations.csv)
  - Detections (detections.csv)
  - Sensordaten (sensor_<typ>.csv)

Standard: PUC-Stationen in Deutschland. Mit --world: alle PUC-Stationen weltweit.

Resume: Jeder Abruf (pro Station und Datentyp) wird als eigene Teildatei in
<out>/<datum>/parts/ gespeichert. Beim nächsten Start werden vorhandene Teile
übersprungen. Die fertigen CSV-Dateien werden am Ende immer aus den Teilen der
aktuell abgefragten Stationen neu zusammengesetzt (auch bei Abbruch mit Strg+C).

Aufruf:
    python birdweather_puc.py                                  # Deutschland, 2026-10-22
    python birdweather_puc.py --world --only-active --workers 4
    python birdweather_puc.py --world --sensors environment light --no-detections
    python birdweather_puc.py --date 2026-10-21 --out daten
    python birdweather_puc.py --fresh                          # Teildateien löschen

Benötigt: pip install requests
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
PAUSE = 0.2  # Sekunden zwischen Seiten-Requests einer Station

# Grobe Bounding Box um Deutschland (enthält auch Randgebiete der Nachbarländer,
# deshalb wird im Deutschland-Modus zusätzlich nach dem Feld "country" gefiltert)
NE = {"lat": 55.1, "lon": 15.1}
SW = {"lat": 47.2, "lon": 5.8}
GERMANY_NAMES = {"germany", "deutschland", "de"}

# Zeitzone, falls bei einer Station keine (gültige) angegeben ist.
# Im Weltmodus wird auf UTC umgestellt, weil Berlin dort falsch wäre.
DEFAULT_TZ = "Europe/Berlin"

_local = threading.local()


def get_session():
    """Eine requests.Session pro Thread (Session ist nicht garantiert thread-sicher)."""
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    return _local.session


# --------------------------------------------------------------------------
# GraphQL-Hilfsfunktionen
# --------------------------------------------------------------------------
def gql(query, variables, retries=4, allow_partial=False):
    """
    POST an die API. Netzwerkfehler werden wiederholt, GraphQL-Fehler nicht.
    Mit allow_partial werden Antworten akzeptiert, die neben Daten auch Fehler
    enthalten (z. B. null in einem Pflichtfeld: die betroffenen Einträge kommen
    dann als None zurück).
    """
    last = None
    for attempt in range(retries):
        try:
            r = get_session().post(URL, json={"query": query, "variables": variables}, timeout=90)
            r.raise_for_status()
            body = r.json()
        except (requests.RequestException, ValueError) as e:
            last = e
            time.sleep(2 ** attempt)
            continue
        if body.get("errors") and not (allow_partial and body.get("data")):
            raise RuntimeError(body["errors"])
        return body["data"]
    raise last


def paginate(query, variables, get_connection, allow_partial=False):
    """Läuft alle Seiten durch und liefert die nodes (bei Teilfehlern auch None)."""
    after = None
    while True:
        data = gql(query, {**variables, "first": PAGE_SIZE, "after": after},
                   allow_partial=allow_partial)
        conn = get_connection(data)
        if conn is None:
            return
        yield from conn["nodes"] or []
        if not conn["pageInfo"]["hasNextPage"]:
            return
        after = conn["pageInfo"]["endCursor"]
        time.sleep(PAUSE)


# --------------------------------------------------------------------------
# Queries
# --------------------------------------------------------------------------
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

# Zeitzone separat: Station.timezone ist in der API als Pflichtfeld deklariert, ist aber
# bei manchen Stationen null. Dann liefert GraphQL die ganze Station als null zurück.
# In einer eigenen, schlanken Abfrage gehen dadurch keine Stationen aus der Hauptliste verloren.
TIMEZONES_Q = """
query($first: Int, $after: String, $ne: InputLocation, $sw: InputLocation) {
  stations(first: $first, after: $after, ne: $ne, sw: $sw) {
    nodes { id timezone }
    pageInfo { hasNextPage endCursor }
  }
}
"""

DETECTIONS_Q = """
query($ids: [ID!], $period: InputDuration, $first: Int, $after: String) {
  detections(stationIds: $ids, period: $period, first: $first, after: $after) {
    nodes {
      id timestamp confidence probability score certainty
      coords { lat lon }
      species { id commonName scientificName }
      soundscape { url startTime endTime duration mode }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

# Sensortyp -> Felder (ohne timestamp, das ist immer dabei)
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


def sensor_query(kind):
    fields = " ".join(["timestamp"] + SENSOR_FIELDS[kind])
    return f"""
query($id: ID!, $period: InputDuration, $first: Int, $after: String) {{
  station(id: $id) {{
    sensors {{
      {kind}History(period: $period, first: $first, after: $after) {{
        nodes {{ {fields} }}
        pageInfo {{ hasNextPage endCursor }}
      }}
    }}
  }}
}}
"""


# --------------------------------------------------------------------------
# Zeit-Hilfsfunktionen
# --------------------------------------------------------------------------
_tzfinder = None


def tz_from_coords(lat, lon):
    """Zeitzone aus Koordinaten, nur wenn das optionale Paket timezonefinder installiert ist."""
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


def station_period(tz, day):
    """
    Abfragezeitraum für den Stationstag. Der lokale Tag wird in UTC umgerechnet
    und um einen Tag nach hinten erweitert (falls "to" exklusiv ist). So ist der
    Tag vollständig enthalten, egal ob die API den Zeitraum in UTC oder in
    Stationszeit auslegt. Zugeschnitten wird danach lokal (on_day).
    """
    start = datetime.combine(day, dtime.min, tzinfo=tz).astimezone(timezone.utc)
    end = datetime.combine(day + timedelta(days=1), dtime.min, tzinfo=tz).astimezone(timezone.utc)
    return {"from": start.date().isoformat(),
            "to": (end.date() + timedelta(days=1)).isoformat()}


def to_local_date(ts, tz):
    """ISO-Zeitstempel -> Datum in der Zeitzone der Station (None bei leerem Wert)."""
    if not ts:
        return None
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(tz).date()


def on_day(ts, tz, day):
    return to_local_date(ts, tz) == day


def is_active_on(station, tz, day):
    """
    Heuristik anhand der Detection-Zeitpunkte der Station:
    aktiv, wenn die letzte Erkennung am oder nach dem Tag liegt und die erste
    nicht nach dem Tag. Stationen ohne jede Erkennung gelten als inaktiv.
    """
    latest = to_local_date(station.get("latestDetectionAt"), tz)
    earliest = to_local_date(station.get("earliestDetectionAt"), tz)
    if latest is None or latest < day:
        return False
    if earliest is not None and earliest > day:
        return False
    return True


# --------------------------------------------------------------------------
# CSV-Ausgabe
# --------------------------------------------------------------------------
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
    """Schreibt erst in eine .tmp-Datei und benennt sie dann um (nie halbe Dateien)."""
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)


def merge_parts(parts_dir, prefix, station_ids, out_path, columns):
    """
    Setzt die Teildateien <prefix>_<station>.csv der angegebenen Stationen zu einer
    Datei zusammen. Teile anderer Stationen (z. B. aus früheren Läufen mit anderem
    Umfang) bleiben außen vor. Gibt die Zeilenzahl zurück.
    """
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


# --------------------------------------------------------------------------
# Abruf einzelner Teile
# --------------------------------------------------------------------------
def fetch_detections(sid, tz, day, period):
    return [flat_detection(d, sid)
            for d in paginate(DETECTIONS_Q, {"ids": [sid], "period": period},
                              lambda data: data["detections"])
            if on_day(d["timestamp"], tz, day)]


def fetch_sensor(kind, sid, tz, day, period):
    def conn(data):
        sensors = (data.get("station") or {}).get("sensors")
        return sensors.get(f"{kind}History") if sensors else None

    return [{"station_id": sid, **r}
            for r in paginate(sensor_query(kind), {"id": sid, "period": period}, conn)
            if on_day(r["timestamp"], tz, day)]


def process_station(s, ctx):
    """Ruft alle noch fehlenden Teile einer Station ab und speichert sie einzeln."""
    args, day, parts = ctx["args"], ctx["day"], ctx["parts"]
    sid, tz = s["id"], station_tz(s)
    period = station_period(tz, day)

    jobs = []
    if not args.no_detections:
        jobs.append(("det", DETECTION_COLS, lambda: fetch_detections(sid, tz, day, period)))
    for kind in args.sensors:
        jobs.append((kind, sensor_cols(kind),
                     lambda kind=kind: fetch_sensor(kind, sid, tz, day, period)))

    todo = [j for j in jobs if not (parts / f"{j[0]}_{sid}.csv").exists()]
    res = {"new": 0, "skipped": len(jobs) - len(todo), "failed": []}
    lines = []

    for part, cols, fn in todo:
        if ctx["stop"].is_set():
            break
        try:
            rows = fn()
        except Exception as e:
            lines.append(f"    {part} FEHLER: {e}")
            res["failed"].append(f"{sid}:{part}")
            continue
        write_csv_atomic(parts / f"{part}_{sid}.csv", cols, rows)
        res["new"] += 1
        if rows:
            lines.append(f"    {part}: {len(rows)}")

    with ctx["lock"]:
        ctx["done"] += 1
        status = "bereits komplett" if not todo else f"{len(todo)}/{len(jobs)} Teile abgerufen"
        elapsed = (time.time() - ctx["t0"]) / 60
        print(f"[{ctx['done']}/{ctx['total']}] Station {sid} {s.get('name')!r}: {status}"
              f"  ({elapsed:.1f} min)")
        for line in lines:
            print(line)
    return res


# --------------------------------------------------------------------------
# Hauptprogramm
# --------------------------------------------------------------------------
def main():
    global DEFAULT_TZ
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-10-22", help="Tag (YYYY-MM-DD), Stationszeit")
    ap.add_argument("--out", default="birdweather_out", help="Ausgabeordner (Unterordner je Datum)")
    ap.add_argument("--world", action="store_true",
                    help="Alle PUC-Stationen weltweit statt nur Deutschland")
    ap.add_argument("--sensors", nargs="+", default=list(SENSOR_FIELDS),
                    choices=list(SENSOR_FIELDS), help="Welche Sensortypen abgerufen werden")
    ap.add_argument("--no-country-filter", action="store_true",
                    help="Deutschland-Modus: nur Bounding Box verwenden, nicht nach Land filtern")
    ap.add_argument("--only-active", action="store_true",
                    help="Stationen auslassen, die am Tag laut Detection-Zeitpunkten inaktiv waren")
    ap.add_argument("--no-detections", action="store_true",
                    help="Keine Detections abrufen (nur Standort und Sensordaten)")
    ap.add_argument("--workers", type=int, default=1,
                    help="Anzahl paralleler Stationsabrufe (Standard 1; bei --world z. B. 4)")
    ap.add_argument("--fresh", action="store_true",
                    help="Vorhandene Teildateien dieses Tages löschen und komplett neu abrufen")
    args = ap.parse_args()

    if args.world:
        DEFAULT_TZ = "UTC"
    day = date.fromisoformat(args.date)

    out = Path(args.out) / day.isoformat()
    parts = out / "parts"
    if args.fresh and parts.exists():
        shutil.rmtree(parts)
    parts.mkdir(parents=True, exist_ok=True)

    # 1) PUC-Stationen laden
    print(f"Lade Stationen ({'weltweit' if args.world else 'Deutschland'}) ...")
    variables = {} if args.world else {"ne": NE, "sw": SW}
    raw = list(paginate(STATIONS_Q, variables, lambda d: d["stations"], allow_partial=True))
    stations = [s for s in raw if s]
    print(f"  {len(stations)} Stationen geladen"
          + (f" ({len(raw) - len(stations)} Einträge nicht lesbar)" if len(raw) != len(stations) else ""))
    print("  Typen:", dict(Counter(s["type"] for s in stations)))

    pucs = [s for s in stations if s["type"] == "puc"]
    countries = Counter(s.get("country") for s in pucs)
    print("  Länder der PUCs:", dict(countries.most_common(15)),
          "..." if len(countries) > 15 else "")
    if not args.world and not args.no_country_filter:
        pucs = [s for s in pucs if (s.get("country") or "").lower() in GERMANY_NAMES]
    print(f"  {len(pucs)} PUC-Stationen nach Filter")
    if not pucs:
        sys.exit("Keine PUC-Stationen gefunden (Länderwerte oben prüfen, ggf. --no-country-filter).")

    # Zeitzonen separat laden (fehlertolerant), nur für die PUCs relevant
    print("Lade Zeitzonen ...")
    tzmap = {s["id"]: s["timezone"]
             for s in paginate(TIMEZONES_Q, variables, lambda d: d["stations"], allow_partial=True)
             if s and s.get("timezone")}
    src = Counter()
    for s in pucs:
        name = tzmap.get(s["id"])
        if name:
            s["timezone"], s["timezone_source"] = name, "api"
        else:
            c = s.get("coords") or {}
            name = tz_from_coords(c.get("lat"), c.get("lon"))
            if name:
                s["timezone"], s["timezone_source"] = name, "coords"
            else:
                s["timezone"], s["timezone_source"] = None, "default"
        src[s["timezone_source"]] += 1
    print(f"  Zeitzone: {src['api']} aus der API, {src['coords']} aus Koordinaten, "
          f"{src['default']} nicht ermittelbar (es wird {DEFAULT_TZ} angenommen)")
    if src["default"] and not src["coords"]:
        print("  Tipp: pip install timezonefinder ermittelt fehlende Zeitzonen aus den Koordinaten")

    # 2) Optional: inaktive Stationen aussortieren
    to_query, station_rows = [], []
    for s in pucs:
        if args.only_active and not is_active_on(s, station_tz(s), day):
            station_rows.append(flat_station(s, "skipped_inactive"))
        else:
            to_query.append(s)
            station_rows.append(flat_station(s, "queried"))
    if args.only_active:
        print(f"  {len(pucs) - len(to_query)} inaktive Stationen übersprungen, "
              f"{len(to_query)} werden abgefragt")

    write_csv_atomic(out / "stations.csv", STATION_COLS, station_rows)

    # 3) Pro Station: Detections + Sensordaten (jeder Teil einzeln gespeichert)
    ctx = {"args": args, "day": day, "parts": parts, "stop": threading.Event(),
           "lock": threading.Lock(), "done": 0, "total": len(to_query), "t0": time.time()}
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
        print("\nAbbruch: warte auf laufende Abrufe, bereits gespeicherte Teile bleiben erhalten ...")
        ex.shutdown(wait=True, cancel_futures=True)

    failed, newly_fetched, already_done = [], 0, 0
    for f in futures:
        if f.done() and not f.cancelled():
            try:
                r = f.result()
            except Exception as e:  # unerwarteter Fehler in einer Station
                failed.append(f"unerwartet: {e}")
                continue
            newly_fetched += r["new"]
            already_done += r["skipped"]
            failed += r["failed"]

    # 4) Endgültige CSV-Dateien aus den Teilen der abgefragten Stationen zusammensetzen
    ids = [s["id"] for s in to_query]
    totals = {}
    if not args.no_detections:
        totals["detections"] = merge_parts(parts, "det", ids, out / "detections.csv", DETECTION_COLS)
    for kind in args.sensors:
        totals[kind] = merge_parts(parts, kind, ids, out / f"sensor_{kind}.csv", sensor_cols(kind))

    print(f"\n{'Zwischenstand' if interrupted else 'Fertig'} nach {(time.time() - t0) / 60:.1f} min.")
    print(f"Teile neu abgerufen: {newly_fetched}, bereits vorhanden: {already_done}")
    print("Zeilen insgesamt:", totals)
    if failed:
        print(f"{len(failed)} Teile fehlgeschlagen ({', '.join(failed[:10])}"
              f"{' ...' if len(failed) > 10 else ''}). Skript erneut starten, um sie nachzuholen.")
    if interrupted:
        print("Zum Fortsetzen denselben Befehl noch einmal ausführen.")
    print(f"Dateien in: {out.resolve()}")
    if interrupted:
        sys.exit(130)


if __name__ == "__main__":
    main()