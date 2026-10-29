#!/usr/bin/env python3
"""
Ruft von allen PUC-Stationen in Deutschland für einen Tag ab:
  - Standort (stations.csv)
  - Detections (detections.csv)
  - Sensordaten (sensor_<typ>.csv)

Resume: Jeder Abruf (pro Station und Datentyp) wird als eigene Teildatei in
<out>/<datum>/parts/ gespeichert. Beim nächsten Start werden vorhandene Teile
übersprungen. Die fertigen CSV-Dateien werden am Ende immer aus allen Teilen
neu zusammengesetzt (auch bei Abbruch mit Strg+C).

Aufruf:
    python birdweather_puc_germany.py                       # 2026-10-22
    python birdweather_puc_germany.py --only-active         # inaktive Stationen auslassen
    python birdweather_puc_germany.py --no-detections       # nur Standort + Sensordaten
    python birdweather_puc_germany.py --date 2026-10-21 --out daten
    python birdweather_puc_germany.py --sensors environment light system
    python birdweather_puc_germany.py --fresh               # Teildateien löschen, neu beginnen

Benötigt: pip install requests
"""
import argparse
import csv
import os
import shutil
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

URL = "https://app.birdweather.com/graphql"
PAGE_SIZE = 500
PAUSE = 0.2  # Sekunden zwischen Requests

# Grobe Bounding Box um Deutschland (enthält auch Randgebiete der Nachbarländer,
# deshalb wird zusätzlich nach dem Feld "country" gefiltert)
NE = {"lat": 55.1, "lon": 15.1}
SW = {"lat": 47.2, "lon": 5.8}
GERMANY_NAMES = {"germany", "deutschland", "de"}

session = requests.Session()


# --------------------------------------------------------------------------
# GraphQL-Hilfsfunktionen
# --------------------------------------------------------------------------
def gql(query, variables, retries=4):
    """POST an die API. Netzwerkfehler werden wiederholt, GraphQL-Fehler nicht."""
    last = None
    for attempt in range(retries):
        try:
            r = session.post(URL, json={"query": query, "variables": variables}, timeout=90)
            r.raise_for_status()
            body = r.json()
        except (requests.RequestException, ValueError) as e:
            last = e
            time.sleep(2 ** attempt)
            continue
        if body.get("errors"):
            raise RuntimeError(body["errors"])
        return body["data"]
    raise last


def paginate(query, variables, get_connection):
    """Läuft alle Seiten durch und liefert die nodes."""
    after = None
    while True:
        data = gql(query, {**variables, "first": PAGE_SIZE, "after": after})
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
      id name type country state location timezone locationPrivacy
      earliestDetectionAt latestDetectionAt
      coords { lat lon }
    }
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
def station_tz(station):
    try:
        return ZoneInfo(station.get("timezone") or "Europe/Berlin")
    except Exception:
        return ZoneInfo("Europe/Berlin")


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
                "latestDetectionAt", "status"]
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


def merge_parts(parts_dir, prefix, out_path, columns):
    """Setzt alle Teildateien <prefix>_*.csv zu einer Datei zusammen. Gibt Zeilenzahl zurück."""
    n = 0
    tmp = out_path.with_suffix(".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for p in sorted(parts_dir.glob(f"{prefix}_*.csv")):
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


# --------------------------------------------------------------------------
# Hauptprogramm
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-10-22", help="Tag (YYYY-MM-DD), Stationszeit")
    ap.add_argument("--out", default="birdweather_out", help="Ausgabeordner (Unterordner je Datum)")
    ap.add_argument("--sensors", nargs="+", default=list(SENSOR_FIELDS),
                    choices=list(SENSOR_FIELDS), help="Welche Sensortypen abgerufen werden")
    ap.add_argument("--no-country-filter", action="store_true",
                    help="Nur Bounding Box verwenden, nicht nach Land filtern")
    ap.add_argument("--only-active", action="store_true",
                    help="Stationen auslassen, die am Tag laut Detection-Zeitpunkten inaktiv waren")
    ap.add_argument("--no-detections", action="store_true",
                    help="Keine Detections abrufen (nur Standort und Sensordaten)")
    ap.add_argument("--fresh", action="store_true",
                    help="Vorhandene Teildateien dieses Tages löschen und komplett neu abrufen")
    args = ap.parse_args()

    day = date.fromisoformat(args.date)
    # Ein Tag Puffer auf beiden Seiten, weil unklar ist, ob "to" inklusive ist und ob
    # der Zeitraum in UTC oder Stationszeit gilt. Zugeschnitten wird später lokal.
    period = {"from": (day - timedelta(days=1)).isoformat(),
              "to": (day + timedelta(days=1)).isoformat()}

    out = Path(args.out) / day.isoformat()
    parts = out / "parts"
    if args.fresh and parts.exists():
        shutil.rmtree(parts)
    parts.mkdir(parents=True, exist_ok=True)

    # 1) Stationen in Deutschland, nur PUC
    print("Lade Stationen ...")
    stations = list(paginate(STATIONS_Q, {"ne": NE, "sw": SW}, lambda d: d["stations"]))
    print(f"  {len(stations)} Stationen in der Bounding Box")
    print("  Typen:", dict(Counter(s["type"] for s in stations)))

    pucs = [s for s in stations if s["type"] == "puc"]
    print("  Länder der PUCs:", dict(Counter(s.get("country") for s in pucs)))
    if not args.no_country_filter:
        pucs = [s for s in pucs if (s.get("country") or "").lower() in GERMANY_NAMES]
    print(f"  {len(pucs)} PUC-Stationen nach Länderfilter")
    if not pucs:
        sys.exit("Keine PUC-Stationen gefunden (Länderwerte oben prüfen, ggf. --no-country-filter).")

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
    jobs_per_station = (0 if args.no_detections else 1) + len(args.sensors)
    failed, newly_fetched, already_done = [], 0, 0
    interrupted = False
    t0 = time.time()

    try:
        for i, s in enumerate(to_query, 1):
            sid, tz = s["id"], station_tz(s)

            jobs = []
            if not args.no_detections:
                jobs.append(("det", DETECTION_COLS,
                             lambda sid=sid, tz=tz: fetch_detections(sid, tz, day, period)))
            for kind in args.sensors:
                jobs.append((kind, sensor_cols(kind),
                             lambda kind=kind, sid=sid, tz=tz: fetch_sensor(kind, sid, tz, day, period)))

            todo = [j for j in jobs if not (parts / f"{j[0]}_{sid}.csv").exists()]
            already_done += len(jobs) - len(todo)
            if not todo:
                print(f"[{i}/{len(to_query)}] Station {sid}: bereits komplett, übersprungen")
                continue

            print(f"[{i}/{len(to_query)}] Station {sid} {s.get('name')!r} "
                  f"({len(todo)}/{jobs_per_station} Teile offen)")
            for part, cols, fn in todo:
                try:
                    rows = fn()
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    print(f"    {part} FEHLER: {e}")
                    failed.append(f"{sid}:{part}")
                    continue
                write_csv_atomic(parts / f"{part}_{sid}.csv", cols, rows)
                newly_fetched += 1
                if rows:
                    print(f"    {part}: {len(rows)}")
    except KeyboardInterrupt:
        interrupted = True
        print("\nAbgebrochen. Bereits gespeicherte Teile bleiben erhalten.")

    # 4) Endgültige CSV-Dateien aus allen Teilen zusammensetzen
    totals = {}
    if not args.no_detections:
        totals["detections"] = merge_parts(parts, "det", out / "detections.csv", DETECTION_COLS)
    for kind in args.sensors:
        totals[kind] = merge_parts(parts, kind, out / f"sensor_{kind}.csv", sensor_cols(kind))

    print(f"\n{'Zwischenstand' if interrupted else 'Fertig'} nach {time.time() - t0:.0f} s.")
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