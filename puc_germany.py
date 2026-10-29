#!/usr/bin/env python3
"""
Ruft von allen PUC-Stationen in Deutschland für einen Tag ab:
  - Standort (stations.csv)
  - Detections (detections.csv)
  - Sensordaten (sensor_<typ>.csv)

Aufruf:
    python birdweather_puc_germany.py                    # 2026-10-22
    python birdweather_puc_germany.py --date 2026-10-21 --out daten
    python birdweather_puc_germany.py --sensors environment light system

Benötigt: pip install requests
"""
import argparse
import csv
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


def on_day(ts, tz, day):
    """True, wenn der Zeitstempel in der Stationszeit auf den Tag 'day' fällt."""
    if not ts:
        return False
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(tz).date() == day


# --------------------------------------------------------------------------
# CSV-Ausgabe
# --------------------------------------------------------------------------
class Csv:
    def __init__(self, path, columns):
        self.f = open(path, "w", newline="", encoding="utf-8")
        self.w = csv.DictWriter(self.f, fieldnames=columns, extrasaction="ignore")
        self.w.writeheader()

    def write(self, rows):
        self.w.writerows(rows)
        self.f.flush()

    def close(self):
        self.f.close()


STATION_COLS = ["id", "name", "type", "country", "state", "location", "timezone",
                "lat", "lon", "locationPrivacy", "earliestDetectionAt", "latestDetectionAt"]
DETECTION_COLS = ["station_id", "id", "timestamp", "confidence", "probability", "score",
                  "certainty", "species_id", "commonName", "scientificName", "lat", "lon",
                  "soundscape_url", "soundscape_start", "soundscape_end",
                  "soundscape_duration", "soundscape_mode"]


def flat_station(s):
    c = s.get("coords") or {}
    return {**{k: s.get(k) for k in STATION_COLS}, "lat": c.get("lat"), "lon": c.get("lon")}


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


# --------------------------------------------------------------------------
# Hauptprogramm
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-10-22", help="Tag (YYYY-MM-DD), Stationszeit")
    ap.add_argument("--out", default="birdweather_out", help="Ausgabeordner")
    ap.add_argument("--sensors", nargs="+", default=list(SENSOR_FIELDS),
                    choices=list(SENSOR_FIELDS), help="Welche Sensortypen abgerufen werden")
    ap.add_argument("--no-country-filter", action="store_true",
                    help="Nur Bounding Box verwenden, nicht nach Land filtern")
    args = ap.parse_args()

    day = date.fromisoformat(args.date)
    # Ein Tag Puffer auf beiden Seiten, weil unklar ist, ob "to" inklusive ist und ob
    # der Zeitraum in UTC oder Stationszeit gilt. Zugeschnitten wird später lokal.
    period = {"from": (day - timedelta(days=1)).isoformat(),
              "to": (day + timedelta(days=1)).isoformat()}

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    # 1) Stationen in Deutschland, nur PUC
    print("Lade Stationen ...")
    stations = list(paginate(STATIONS_Q, {"ne": NE, "sw": SW}, lambda d: d["stations"]))
    print(f"  {len(stations)} Stationen in der Bounding Box")
    print("  Typen:", dict(Counter(s["type"] for s in stations)))

    pucs = [s for s in stations if s["type"] == "puc"]
    print("  Länder der PUCs:", dict(Counter(s.get("country") for s in pucs)))
    if not args.no_country_filter:
        pucs = [s for s in pucs if (s.get("country") or "").lower() in GERMANY_NAMES]
    print(f"  {len(pucs)} PUC-Stationen nach Filter")
    if not pucs:
        sys.exit("Keine PUC-Stationen gefunden (Länderwerte oben prüfen, ggf. --no-country-filter).")

    st_csv = Csv(out / "stations.csv", STATION_COLS)
    st_csv.write([flat_station(s) for s in pucs])
    st_csv.close()

    det_csv = Csv(out / "detections.csv", DETECTION_COLS)
    sensor_csv = {k: Csv(out / f"sensor_{k}.csv", ["station_id", "timestamp"] + SENSOR_FIELDS[k])
                  for k in args.sensors}

    # 2) Pro Station: Detections + Sensordaten
    totals = Counter()
    for i, s in enumerate(pucs, 1):
        sid, tz = s["id"], station_tz(s)
        print(f"[{i}/{len(pucs)}] Station {sid} {s.get('name')!r}")

        try:
            dets = [flat_detection(d, sid)
                    for d in paginate(DETECTIONS_Q, {"ids": [sid], "period": period},
                                      lambda d: d["detections"])
                    if on_day(d["timestamp"], tz, day)]
            det_csv.write(dets)
            totals["detections"] += len(dets)
            print(f"    detections: {len(dets)}")
        except Exception as e:
            print(f"    detections FEHLER: {e}")

        for kind in args.sensors:
            try:
                def conn(data, kind=kind):
                    sensors = (data.get("station") or {}).get("sensors")
                    return sensors.get(f"{kind}History") if sensors else None

                rows = [{"station_id": sid, **r}
                        for r in paginate(sensor_query(kind), {"id": sid, "period": period}, conn)
                        if on_day(r["timestamp"], tz, day)]
                sensor_csv[kind].write(rows)
                totals[kind] += len(rows)
                if rows:
                    print(f"    {kind}: {len(rows)}")
            except Exception as e:
                print(f"    {kind} FEHLER: {e}")

    det_csv.close()
    for c in sensor_csv.values():
        c.close()

    print("\nFertig. Zeilen insgesamt:", dict(totals))
    print(f"Dateien in: {out.resolve()}")


if __name__ == "__main__":
    main()