#!/usr/bin/env python3
"""
Prüft die Datenverfügbarkeit von BirdWeather-PUC-Stationen im CONUS-Gebiet
(2024 bis heute) gezielt für aqi, eco2 und tvoc mittels saisonal gestaffelter
Stichproben (inkl. Lückenanalyse sowie erstem/letztem Datenpunkt je Station).

Benötigt birdweather_puc.py im selben Ordner.
"""
import argparse
import csv
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

from birdweather_puc import STATIONS_Q, gql, paginate

CONUS_SW = {"lat": 24.396308, "lng": -125.0}
CONUS_NE = {"lat": 49.384358, "lng": -66.93457}
US_NAMES = {"united states", "united states of america", "us", "usa"}
NON_CONUS_STATES = {"ak", "alaska", "hi", "hawaii", "pr", "puerto rico", "gu", "vi"}

DEFAULT_ENV_VARS = ["aqi", "eco2", "tvoc"]


# --------------------------------------------------------------------------
# Stichprobenplan & Zeitraum
# --------------------------------------------------------------------------
def build_sample_dates(
    start_day: date,
    end_day: date,
    fire_months: set[int],
    fire_step: int,
    off_step: int,
    density_factor: int = 1,
) -> list[date]:
    eff_fire_step = max(1, round(fire_step / density_factor))
    eff_off_step = max(1, round(off_step / density_factor))

    dates = []
    cur = start_day
    fire_counter = 0
    off_counter = 0

    while cur <= end_day:
        if cur.month in fire_months:
            if fire_counter % eff_fire_step == 0:
                dates.append(cur)
            fire_counter += 1
            off_counter = 0
        else:
            if off_counter % eff_off_step == 0:
                dates.append(cur)
            off_counter += 1
            fire_counter = 0
        cur += timedelta(days=1)

    return dates


def window(day: date, inclusive: bool) -> tuple[str, str]:
    to = day if inclusive else day + timedelta(days=1)
    return day.isoformat(), to.isoformat()


def detect_to_inclusive(stations: list[dict], probes: int = 5) -> bool | None:
    cands = []
    for s in stations:
        ts = s.get("latestDetectionAt")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if 9 <= dt.hour <= 15:
            cands.append((dt, s))
    cands.sort(key=lambda x: x[0], reverse=True)
    tested = 0
    for dt, s in cands[:probes]:
        d = dt.date().isoformat()
        q = (
            f'query {{ station(id: {json.dumps(str(s["id"]))}) '
            f'{{ counts(period: {{from: "{d}", to: "{d}"}}) {{ detections }} }} }}'
        )
        try:
            n = gql(q, {}, retries=2)["station"]["counts"]["detections"]
        except Exception:
            continue
        tested += 1
        if n > 0:
            return True
    return False if tested else None


def probe_env_fields(stations: list[dict], env_vars: list[str]) -> bool:
    """Prüft einmalig am Schema, ob nodes { aqi eco2 tvoc } abgefragt werden kann."""
    if not stations:
        return False
    sid = json.dumps(str(stations[0]["id"]))
    fields_str = " ".join(env_vars)
    q = f"query {{ station(id: {sid}) {{ sensors {{ environmentHistory(first: 1) {{ totalCount nodes {{ {fields_str} }} }} }} }} }}"
    try:
        gql(q, {}, retries=1)
        return True
    except Exception:
        return False


# --------------------------------------------------------------------------
# GraphQL-Batching & Cache
# --------------------------------------------------------------------------
def build_query(batch, env_vars: list[str], use_nodes: bool, inclusive: bool) -> str:
    parts = []
    node_block = f"nodes {{ {' '.join(env_vars)} }}" if (use_nodes and env_vars) else ""
    for i, (s, dates) in enumerate(batch):
        sens = []
        for j, d in enumerate(dates):
            f, t = window(d, inclusive)
            per = f'{{from: "{f}", to: "{t}"}}'
            sens.append(f"env_{j}: environmentHistory(period: {per}, first: 3) {{ totalCount {node_block} }}")
        parts.append(
            f"s{i}: station(id: {json.dumps(str(s['id']))}) "
            f"{{ sensors {{ {' '.join(sens)} }} }}"
        )
    return "query { " + " ".join(parts) + " }"


def run_batch(batch, env_vars: list[str], use_nodes: bool, inclusive: bool):
    try:
        data = gql(build_query(batch, env_vars, use_nodes, inclusive), {}, retries=2)
        results = []
        for i, (s, dates) in enumerate(batch):
            st = data.get(f"s{i}")
            if st is None:
                raise RuntimeError(f"Station {s['id']}: leere Antwort")
            sens = st.get("sensors") or {}
            for j, d in enumerate(dates):
                hist = sens.get(f"env_{j}") or {}
                total = hist.get("totalCount") or 0
                rec = {"environment": total}
                if use_nodes:
                    nodes = hist.get("nodes") or []
                    for v in env_vars:
                        rec[v] = 1 if any(n and n.get(v) is not None for n in nodes) else 0
                else:
                    for v in env_vars:
                        rec[v] = 1 if total > 0 else 0
                results.append((str(s["id"]), d, rec))
        return results, []
    except Exception as e:
        if len(batch) == 1:
            return [], [(str(batch[0][0]["id"]), str(e)[:200])]
        mid = len(batch) // 2
        r1, f1 = run_batch(batch[:mid], env_vars, use_nodes, inclusive)
        r2, f2 = run_batch(batch[mid:], env_vars, use_nodes, inclusive)
        return r1 + r2, f1 + f2


def load_cache(path: Path, win: str) -> dict:
    cache = {}
    if path.exists():
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r.get("win") == win:
                    cache[(str(r["sid"]), r["date"])] = r["counts"]
    return cache


def load_preselected_ids(summary_csv: Path, min_avail_pct: float, target_vars: list[str]) -> set[str]:
    selected = set()
    with open(summary_csv, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if target_vars and row.get("sensor") not in target_vars:
                continue
            try:
                avail = float(row.get("availability_active_span_pct") or 0.0)
            except ValueError:
                avail = 0.0
            if avail >= min_avail_pct:
                selected.add(str(row["id"]))
    return selected


# --------------------------------------------------------------------------
# Lückenanalyse & Reporting
# --------------------------------------------------------------------------
def analyze_and_report(
    stations: list[dict],
    dates: list[date],
    cache: dict,
    env_vars: list[str],
    fire_months: set[int],
    fire_step: int,
    off_step: int,
    out: Path,
):
    keys = [d.isoformat() for d in dates]
    kinds = list(env_vars)
    summary_rows = []
    gap_rows = []

    for s in stations:
        sid = str(s["id"])
        coords = s.get("coords") or {}
        for kind in kinds:
            flags = [((cache.get((sid, d)) or {}).get(kind) or 0) > 0 for d in keys]
            total_slots = len(dates)
            active_slots = sum(flags)
            fire_indices = [i for i, dt in enumerate(dates) if dt.month in fire_months]
            fire_hits = sum(1 for i in fire_indices if flags[i])
            fire_avail_pct = round(100.0 * fire_hits / len(fire_indices), 1) if fire_indices else 0.0

            if active_slots == 0:
                first_seen = ""
                last_seen = ""
                avail_total_pct = 0.0
                avail_span_pct = 0.0
                internal_gaps_count = 0
                max_internal_gap_days = 0.0
                first_idx = None
                last_idx = None
            else:
                active_indices = [i for i, ok in enumerate(flags) if ok]
                first_idx = active_indices[0]
                last_idx = active_indices[-1]
                first_seen = keys[first_idx]
                last_seen = keys[last_idx]

                span_len = last_idx - first_idx + 1
                span_hits = sum(flags[first_idx : last_idx + 1])
                avail_total_pct = round(100.0 * active_slots / total_slots, 1)
                avail_span_pct = round(100.0 * span_hits / span_len, 1)
                internal_gaps_count = 0
                max_internal_gap_days = 0.0

            i = 0
            while i < total_slots:
                if flags[i]:
                    i += 1
                    continue
                j = i
                while j + 1 < total_slots and not flags[j + 1]:
                    j += 1

                if first_idx is None:
                    gap_type = "never_active"
                elif j < first_idx:
                    gap_type = "leading"
                elif i > last_idx:
                    gap_type = "trailing"
                else:
                    gap_type = "internal"

                step_d = float(fire_step if dates[i].month in fire_months else off_step)
                approx_days = round((dates[j] - dates[i]).days + step_d, 1)
                overlaps_fire = any(dates[k].month in fire_months for k in range(i, j + 1))

                if gap_type == "internal":
                    internal_gaps_count += 1
                    max_internal_gap_days = max(max_internal_gap_days, approx_days)

                gap_rows.append({
                    "id": sid,
                    "name": s.get("name"),
                    "state": s.get("state"),
                    "sensor": kind,
                    "gap_type": gap_type,
                    "gap_start": keys[i],
                    "gap_end": keys[j],
                    "missing_samples": j - i + 1,
                    "approx_gap_days": approx_days,
                    "overlaps_fire_season": overlaps_fire,
                })
                i = j + 1

            summary_rows.append({
                "id": sid,
                "name": s.get("name"),
                "country": s.get("country"),
                "state": s.get("state"),
                "lat": coords.get("lat"),
                "lon": coords.get("lng") or coords.get("lon"),
                "sensor": kind,
                "first_seen": first_seen,
                "last_seen": last_seen,
                "total_samples": total_slots,
                "active_samples": active_slots,
                "availability_total_pct": avail_total_pct,
                "availability_active_span_pct": avail_span_pct,
                "fire_season_avail_pct": fire_avail_pct,
                "internal_gaps_count": internal_gaps_count,
                "max_internal_gap_days": max_internal_gap_days,
                "earliestDetectionAt": s.get("earliestDetectionAt"),
                "latestDetectionAt": s.get("latestDetectionAt"),
            })

    summary_rows.sort(key=lambda r: (r["sensor"], -r["active_samples"], str(r["id"])))

    sum_path = out / "birdweather_conus_stations_summary.csv"
    with open(sum_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        w.writeheader()
        w.writerows(summary_rows)

    gap_path = out / "birdweather_conus_gaps.csv"
    if gap_rows:
        with open(gap_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(gap_rows[0].keys()))
            w.writeheader()
            w.writerows(gap_rows)

    print(f"\n=== BirdWeather CONUS-Zusammenfassung ({len(stations)} PUC-Stationen, {len(dates)} Tage) ===")
    for kind in kinds:
        sub = [r for r in summary_rows if r["sensor"] == kind]
        active_any = sum(1 for r in sub if r["active_samples"] > 0)
        high_qual = sum(1 for r in sub if r["active_samples"] > 0 and r["availability_active_span_pct"] >= 80.0)
        print(f"  Variable '{kind}': {active_any} Stationen aktiv, davon {high_qual} mit >=80% Verfügbarkeit (aktive Spanne)")
    print(f"\nGespeichert:\n  - {sum_path}\n  - {gap_path}")


# --------------------------------------------------------------------------
# Hauptprogramm
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="CONUS Vorselektion für BirdWeather PUC (aqi, eco2, tvoc)")
    ap.add_argument("--start", default="2024-01-01", help="Startdatum (YYYY-MM-DD, Standard: 2024-01-01)")
    ap.add_argument("--end", default=date.today().isoformat(), help="Enddatum (YYYY-MM-DD, Standard: heute)")
    ap.add_argument("--fire-months", nargs="+", type=int, default=[5, 6, 7, 8, 9, 10],
                    help="Waldbrandmonate 1..12 (Standard: 5 6 7 8 9 10)")
    ap.add_argument("--fire-step", type=int, default=1, help="Schrittweite in Tagen (Waldbrandmonate, Standard: 1)")
    ap.add_argument("--off-step", type=int, default=3, help="Schrittweite in Tagen (Nebensaison, Standard: 3)")
    ap.add_argument("--dense", action="store_true", help="Verdichteter Lauf (entspricht --density-factor 2)")
    ap.add_argument("--density-factor", type=int, default=1, help="Verdichtungsfaktor (z. B. 2)")
    ap.add_argument("--env-vars", nargs="+", default=DEFAULT_ENV_VARS,
                    help="Zu prüfende Environment-Felder (Standard: aqi eco2 tvoc)")
    ap.add_argument("--station-ids", nargs="+", help="Nur diese Stations-IDs prüfen")
    ap.add_argument("--from-summary", type=Path, help="Vorselektion aus Summary-CSV laden")
    ap.add_argument("--min-avail", type=float, default=50.0,
                    help="Mindestverfügbarkeit (%%) bei --from-summary (Standard: 50.0)")
    ap.add_argument("--batch", type=int, default=8, help="Stationen pro GraphQL-Request (Standard: 8)")
    ap.add_argument("--workers", type=int, default=3, help="Parallele Requests (Standard: 3)")
    ap.add_argument("--to-inclusive", choices=["auto", "yes", "no"], default="auto")
    ap.add_argument("--out", default="birdweather_conus_check", help="Ausgabeordner")
    ap.add_argument("--fresh", action="store_true", help="Cache löschen und neu starten")
    args = ap.parse_args()

    factor = max(args.density_factor, 2 if args.dense else 1)
    start_day = date.fromisoformat(args.start)
    end_day = min(date.fromisoformat(args.end), date.today())
    fire_months = set(args.fire_months)

    dates = build_sample_dates(start_day, end_day, fire_months, args.fire_step, args.off_step, factor)
    eff_fire_step = max(1, round(args.fire_step / factor))
    eff_off_step = max(1, round(args.off_step / factor))
    print(f"Stichprobenplan: {len(dates)} Tage ({start_day} bis {end_day}, "
          f"Waldbrand={eff_fire_step}d, Nebensaison={eff_off_step}d, Faktor={factor}x)")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    cache_path = out / "check_cache.jsonl"
    if args.fresh and cache_path.exists():
        cache_path.unlink()

    print("Lade CONUS PUC-Stationen ...")
    raw = list(paginate(STATIONS_Q, {"ne": CONUS_NE, "sw": CONUS_SW}, lambda d: d["stations"], allow_partial=True))
    stations = []
    for s in raw:
        if not s or s.get("type") != "puc":
            continue
        country = (s.get("country") or "").strip().lower()
        state = (s.get("state") or "").strip().lower()
        if country and country not in US_NAMES:
            continue
        if state in NON_CONUS_STATES:
            continue
        stations.append(s)

    allowed_ids = set(args.station_ids or [])
    if args.from_summary:
        allowed_ids |= load_preselected_ids(args.from_summary, args.min_avail, args.env_vars)
    if allowed_ids:
        stations = [s for s in stations if str(s["id"]) in allowed_ids]

    stations.sort(key=lambda s: str(s["id"]))
    print(f"  {len(stations)} CONUS PUC-Stationen im Prüfscope")
    if not stations:
        sys.exit("Keine passenden Stationen gefunden.")

    if args.to_inclusive == "auto":
        inc = detect_to_inclusive(stations)
        inc = False if inc is None else inc
    else:
        inc = args.to_inclusive == "yes"
    win = "inc" if inc else "exc"

    use_nodes = probe_env_fields(stations, args.env_vars)
    if not use_nodes:
        print("Hinweis: Direkte Feldabfrage (aqi/eco2/tvoc) nicht vom Schema akzeptiert – nutze environmentHistory.totalCount.")

    cache = load_cache(cache_path, win)
    pending = []
    for s in stations:
        sid = str(s["id"])
        missing = [d for d in dates if not all(k in cache.get((sid, d.isoformat()), {}) for k in args.env_vars)]
        if missing:
            pending.append((s, missing))

    print(f"  {len(stations) - len(pending)} Stationen bereits im Cache, {len(pending)} offen")

    effective_batch = min(args.batch, max(1, 2000 // max(1, len(dates))))
    batches = [pending[i : i + effective_batch] for i in range(0, len(pending), effective_batch)]
    failures, t0 = [], time.time()

    if batches:
        with open(cache_path, "a", encoding="utf-8") as cache_f, ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = [ex.submit(run_batch, b, args.env_vars, use_nodes, inc) for b in batches]
            for n, f in enumerate(as_completed(futures), 1):
                results, fails = f.result()
                for sid, d, rec in results:
                    key = (sid, d.isoformat())
                    cache.setdefault(key, {}).update(rec)
                    cache_f.write(json.dumps({"sid": sid, "date": d.isoformat(), "win": win, "counts": cache[key]}) + "\n")
                cache_f.flush()
                failures += fails
                if n % 5 == 0 or n == len(futures):
                    print(f"  [{n}/{len(futures)}] Batches fertig, {len(failures)} Fehler, {(time.time() - t0)/60:.1f} min")

    analyze_and_report(
        stations, dates, cache, args.env_vars,
        fire_months, eff_fire_step, eff_off_step, out
    )


if __name__ == "__main__":
    main()