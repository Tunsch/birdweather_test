#!/usr/bin/env python3
"""
Prüft, wie viele PUC-Stationen an bestimmten Tagen Sensordaten und/oder Detections
haben, OHNE die Daten selbst abzurufen. Es werden nur Zähler abgefragt
(totalCount der Sensor-Historien, Station.counts für Detections).

Mehrere Stationen und Tage werden per GraphQL-Aliasen in einem Request gebündelt.
Lehnt der Server eine Abfrage ab (zu groß/zu komplex), wird der Stapel automatisch
halbiert und erneut versucht. Ergebnisse werden zwischengespeichert (Resume).

Benötigt birdweather_puc.py im selben Ordner (nutzt dessen API-Funktionen).

Aufruf:
    python birdweather_check.py --quarters 2025                  # 4 Tage: 15.2., 15.5., 15.8., 15.11.
    python birdweather_check.py --dates 2025-03-01 2025-06-01 2025-09-01 2025-12-01 --world
    python birdweather_check.py --quarters 2025 --world --workers 4 --batch 10
"""
import argparse
import csv
import json
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

from birdweather_puc import GERMANY_NAMES, NE, SENSOR_FIELDS, STATIONS_Q, SW, gql, paginate


# --------------------------------------------------------------------------
# Zeitraum
# --------------------------------------------------------------------------
def window(day, inclusive):
    """
    Zeitraum für einen Tag. Ist "to" inklusive, genügt from = to = Tag.
    Sonst (exklusiv) wird bis zum Folgetag gefragt, was genau den Tag ergibt.
    """
    to = day if inclusive else day + timedelta(days=1)
    return day.isoformat(), to.isoformat()


def detect_to_inclusive(stations, probes=5):
    """
    Findet heraus, ob die API "to" inklusive auslegt: Für Stationen mit einer
    Erkennung um die Mittagszeit wird der Tag dieser Erkennung mit from = to
    abgefragt. Ergibt das > 0, ist "to" inklusive. True / False / None (unklar).
    """
    cands = []
    for s in stations:
        ts = s.get("latestDetectionAt")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if 9 <= dt.hour <= 15:      # Mittag: Tag in UTC und Stationszeit identisch
            cands.append((dt, s))
    cands.sort(key=lambda x: x[0], reverse=True)
    tested = 0
    for dt, s in cands[:probes]:
        d = dt.date().isoformat()
        q = (f'query {{ station(id: {json.dumps(str(s["id"]))}) '
             f'{{ counts(period: {{from: "{d}", to: "{d}"}}) {{ detections }} }} }}')
        try:
            n = gql(q, {}, retries=2)["station"]["counts"]["detections"]
        except Exception:
            continue
        tested += 1
        if n > 0:
            return True
    return False if tested else None


# --------------------------------------------------------------------------
# Abfrage
# --------------------------------------------------------------------------
def build_query(batch, sensors, with_det, inclusive):
    """batch: Liste von (Station, [Tage]). Pro Station/Tag je ein Zähler pro Datentyp."""
    parts = []
    for i, (s, dates) in enumerate(batch):
        sens, other = [], []
        for j, d in enumerate(dates):
            f, t = window(d, inclusive)
            per = f'{{from: "{f}", to: "{t}"}}'
            for kind in sensors:
                sens.append(f"{kind}_{j}: {kind}History(period: {per}, first: 1) {{ totalCount }}")
            if with_det:
                other.append(f"det_{j}: counts(period: {per}) {{ detections }}")
        sens_block = f"sensors {{ {' '.join(sens)} }}" if sens else ""
        parts.append(f"s{i}: station(id: {json.dumps(str(s['id']))}) "
                     f"{{ {sens_block} {' '.join(other)} }}")
    return "query { " + " ".join(parts) + " }"


def run_batch(batch, sensors, with_det, inclusive):
    """
    Führt einen Stapel aus. Bei Fehlern wird der Stapel halbiert, bis nur noch eine
    Station übrig ist. Gibt (Ergebnisse, Fehler) zurück.
    Ergebnisse: Liste (station_id, tag, {typ: anzahl}); Fehler: Liste (station_id, text).
    """
    try:
        data = gql(build_query(batch, sensors, with_det, inclusive), {}, retries=2)
        results = []
        for i, (s, dates) in enumerate(batch):
            st = data.get(f"s{i}")
            if st is None:
                raise RuntimeError(f"Station {s['id']}: leere Antwort")
            sens = st.get("sensors")
            for j, d in enumerate(dates):
                rec = {kind: (sens[f"{kind}_{j}"]["totalCount"] if sens else 0) for kind in sensors}
                if with_det:
                    rec["det"] = st[f"det_{j}"]["detections"]
                results.append((s["id"], d, rec))
        return results, []
    except Exception as e:
        if len(batch) == 1:
            return [], [(batch[0][0]["id"], str(e)[:200])]
        mid = len(batch) // 2
        r1, f1 = run_batch(batch[:mid], sensors, with_det, inclusive)
        r2, f2 = run_batch(batch[mid:], sensors, with_det, inclusive)
        return r1 + r2, f1 + f2


# --------------------------------------------------------------------------
# Cache
# --------------------------------------------------------------------------
def load_cache(path, win):
    cache = {}
    if path.exists():
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r.get("win") == win:
                    cache[(r["sid"], r["date"])] = r["counts"]
    return cache


# --------------------------------------------------------------------------
# Auswertung
# --------------------------------------------------------------------------
def has_sensor(rec, sensors):
    return any((rec.get(k) or 0) > 0 for k in sensors)


def report(stations, dates, cache, sensors, with_det, out):
    keys = [d.isoformat() for d in dates]
    # --- Zusammenfassung pro Tag
    summary = []
    for d in keys:
        recs = [cache.get((s["id"], d)) for s in stations]
        known = [r for r in recs if r]
        row = {"date": d, "stations_total": len(stations), "checked": len(known),
               "unknown": len(recs) - len(known)}
        for kind in sensors:
            row[f"with_{kind}"] = sum(1 for r in known if (r.get(kind) or 0) > 0)
        row["with_any_sensor"] = sum(1 for r in known if has_sensor(r, sensors))
        if with_det:
            row["with_detections"] = sum(1 for r in known if (r.get("det") or 0) > 0)
            row["sensor_and_detections"] = sum(1 for r in known
                                               if has_sensor(r, sensors) and (r.get("det") or 0) > 0)
            row["sensor_or_detections"] = sum(1 for r in known
                                              if has_sensor(r, sensors) or (r.get("det") or 0) > 0)
        summary.append(row)

    with open(out / "check_summary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)

    # --- Pro Station
    cols = ["id", "name", "country", "state", "lat", "lon",
            "earliestDetectionAt", "latestDetectionAt"]
    kinds = list(sensors) + (["det"] if with_det else [])
    header = cols + [f"{k}_{d}" for d in keys for k in kinds]
    with open(out / "check_stations.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for s in stations:
            c = s.get("coords") or {}
            base = [s["id"], s.get("name"), s.get("country"), s.get("state"),
                    c.get("lat"), c.get("lon"), s.get("earliestDetectionAt"),
                    s.get("latestDetectionAt")]
            vals = []
            for d in keys:
                r = cache.get((s["id"], d)) or {}
                vals += [r.get(k, "") for k in kinds]
            w.writerow(base + vals)

    # --- Ausgabe auf der Konsole
    print("\n=== Ergebnis pro Tag ===")
    labels = [("checked", "geprüft")] + [(f"with_{k}", k) for k in sensors] + [("with_any_sensor", "Sensor")]
    if with_det:
        labels += [("with_detections", "Detections"), ("sensor_and_detections", "Sensor+Det"),
                   ("sensor_or_detections", "Sensor|Det")]
    labels.append(("unknown", "unbekannt"))
    print(f"{'Datum':12s}" + "".join(f"{lab:>13s}" for _, lab in labels))
    for row in summary:
        print(f"{row['date']:12s}" + "".join(f"{row[k]:>13d}" for k, _ in labels))

    # --- Über alle Tage
    def active(rec):
        return rec and (has_sensor(rec, sensors) or (with_det and (rec.get("det") or 0) > 0))

    per_station = [[active(cache.get((s["id"], d))) for d in keys] for s in stations]
    at_least_once = sum(1 for a in per_station if any(a))
    in_all = sum(1 for a in per_station if all(a))
    print(f"\nStationen im Umfang: {len(stations)}")
    print(f"Mit Sensordaten{' oder Detections' if with_det else ''} an mindestens einem der "
          f"{len(keys)} Tage: {at_least_once}")
    print(f"An allen {len(keys)} Tagen: {in_all}")
    print("Hinweis: Das ist eine Stichprobe. Stationen, die nur an anderen Tagen des Jahres "
          "aktiv waren, werden nicht erfasst. Der Wert ist eine Untergrenze.")
    print(f"\nDateien: {out / 'check_summary.csv'}, {out / 'check_stations.csv'}")


# --------------------------------------------------------------------------
# Hauptprogramm
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", nargs="+", help="Tage (YYYY-MM-DD)")
    ap.add_argument("--quarters", type=int, metavar="JAHR",
                    help="je Quartal ein Tag des Jahres (15.2., 15.5., 15.8., 15.11.)")
    ap.add_argument("--range", nargs=2, metavar=("START", "ENDE"), help="Zeitraum (inklusive, YYYY-MM-DD)")
    ap.add_argument("--year", type=int, metavar="JAHR", help="Ganzes Jahr abfragen")
    ap.add_argument("--world", action="store_true", help="Weltweit statt nur Deutschland")
    ap.add_argument("--sensors", nargs="+", default=["environment", "light"],
                    choices=list(SENSOR_FIELDS), help="Zu prüfende Sensortypen")
    ap.add_argument("--no-detections", action="store_true", help="Detections nicht zählen")
    ap.add_argument("--batch", type=int, default=10, help="Stationen pro Request (Standard 10)")
    ap.add_argument("--workers", type=int, default=3, help="Parallele Requests (Standard 3)")
    ap.add_argument("--to-inclusive", choices=["auto", "yes", "no"], default="auto",
                    help='Ob die API "to" inklusive auslegt (auto = per Probe ermitteln)')
    ap.add_argument("--out", default="birdweather_check", help="Ausgabeordner")
    ap.add_argument("--fresh", action="store_true", help="Zwischenspeicher löschen")
    args = ap.parse_args()

    # Tage bestimmen
    dates = set()

    if getattr(args, "range", None):
        start = date.fromisoformat(args.range[0])
        end = date.fromisoformat(args.range[1])
        dates |= {start + timedelta(days=i) for i in range((end - start).days + 1)}
    if getattr(args, "year", None):
        start = date(args.year, 1, 1)
        end = date(args.year, 12, 31)
        dates |= {start + timedelta(days=i) for i in range((end - start).days + 1)}

    if args.dates:
        dates |= {date.fromisoformat(d) for d in args.dates}
    if args.quarters:
        dates |= {date(args.quarters, m, 15) for m in (2, 5, 8, 11)}
    if not dates:
        sys.exit("Bitte --dates, --quarters, --range oder --year angeben.")
    future = {d for d in dates if d > date.today()}
    if future:
        print("Übersprungen (liegt in der Zukunft):", ", ".join(sorted(d.isoformat() for d in future)))
    dates = sorted(dates - future)
    if not dates:
        sys.exit("Keine gültigen Tage übrig.")
    with_det = not args.no_detections
    sensors = args.sensors

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    cache_path = out / "check_cache.jsonl"
    if args.fresh and cache_path.exists():
        cache_path.unlink()

    # Stationen laden (ohne timezone: Pflichtfeld in der API, bei manchen Stationen null)
    print(f"Lade Stationen ({'weltweit' if args.world else 'Deutschland'}) ...")
    variables = {} if args.world else {"ne": NE, "sw": SW}
    raw = list(paginate(STATIONS_Q, variables, lambda d: d["stations"], allow_partial=True))
    stations = [s for s in raw if s and s["type"] == "puc"]
    if not args.world:
        stations = [s for s in stations if (s.get("country") or "").lower() in GERMANY_NAMES]
    stations.sort(key=lambda s: str(s["id"]))
    print(f"  {len(stations)} PUC-Stationen")
    if not stations:
        sys.exit("Keine PUC-Stationen gefunden.")
    ever = sum(1 for s in stations if s.get("latestDetectionAt"))
    print(f"  davon mit mindestens einer Erkennung insgesamt: {ever}")

    # Auslegung von "to" klären
    if args.to_inclusive == "auto":
        inc = detect_to_inclusive(stations)
        if inc is None:
            print('  "to" konnte nicht ermittelt werden, es wird exklusiv angenommen '
                  "(Zeitraum Tag bis Folgetag, kann den Folgetag teilweise mitzählen)")
            inc = False
        else:
            print(f'  "to" ist {"inklusive" if inc else "exklusiv"} (per Probe ermittelt)')
    else:
        inc = args.to_inclusive == "yes"
    win = "inc" if inc else "exc"

    # Was fehlt noch? (Resume)
    cache = load_cache(cache_path, win)
    needed = list(sensors) + (["det"] if with_det else [])
    pending = []
    for s in stations:
        missing = [d for d in dates
                   if not all(k in cache.get((s["id"], d.isoformat()), {}) for k in needed)]
        if missing:
            pending.append((s, missing))
    done = len(stations) - len(pending)
    print(f"  {done} Stationen bereits vollständig geprüft, {len(pending)} offen")

    batches = [pending[i:i + args.batch] for i in range(0, len(pending), args.batch)]
    failures, t0 = [], time.time()
    processed = set()
    interrupted = False

    if batches:
        cache_f = open(cache_path, "a", encoding="utf-8")
        ex = ThreadPoolExecutor(max_workers=max(1, args.workers))
        futures = [ex.submit(run_batch, b, sensors, with_det, inc) for b in batches]
        try:
            for n, f in enumerate(as_completed(futures), 1):
                processed.add(f)
                results, fails = f.result()
                for sid, d, rec in results:
                    key = (sid, d.isoformat())
                    cache.setdefault(key, {}).update(rec)
                    cache_f.write(json.dumps({"sid": sid, "date": d.isoformat(), "win": win,
                                              "counts": cache[key]}) + "\n")
                cache_f.flush()
                failures += fails
                if n % 5 == 0 or n == len(futures):
                    print(f"  [{n}/{len(futures)}] Stapel fertig, {len(failures)} Fehler, "
                          f"{(time.time() - t0) / 60:.1f} min")
            ex.shutdown()
        except KeyboardInterrupt:
            interrupted = True
            print("\nAbbruch: warte auf laufende Abfragen, Ergebnisse bis hier bleiben gespeichert ...")
            ex.shutdown(wait=True, cancel_futures=True)
            for f in futures:        # noch fertig gewordene Stapel sichern
                if f.done() and not f.cancelled() and f not in processed:
                    try:
                        results, fails = f.result()
                    except Exception:
                        continue
                    for sid, d, rec in results:
                        key = (sid, d.isoformat())
                        if key not in cache:
                            cache[key] = dict(rec)
                            cache_f.write(json.dumps({"sid": sid, "date": d.isoformat(), "win": win,
                                                      "counts": cache[key]}) + "\n")
                    failures += fails
            cache_f.flush()
        cache_f.close()

    if failures:
        print(f"\n{len(failures)} Stationen konnten nicht geprüft werden, z. B.: {failures[0]}")
        print("Beim nächsten Start werden sie erneut versucht.")

    report(stations, dates, cache, sensors, with_det, out)
    if interrupted:
        print("\nZwischenstand. Zum Fortsetzen denselben Befehl noch einmal ausführen.")
        sys.exit(130)


if __name__ == "__main__":
    main()
