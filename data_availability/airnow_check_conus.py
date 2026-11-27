#!/usr/bin/env python3
"""
Ressourcenschonende Vorselektion von AirNow-Luftqualitätsstationen im CONUS-Gebiet
(2024 bis heute) für PM2.5, PM10 und Ozon (O3) über stündliche Stichproben.
"""
import argparse
import csv
import json
import os
import time
from collections import deque
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

DEFAULT_API_KEY = os.getenv("AIRNOW_API_KEY", "56BAF77F-6DB0-45DC-99CC-A93BE3FF6F69")
CONUS_BBOX = "-125.0,24.396308,-66.93457,49.384358"


def normalize_param(p: str) -> str:
    p_up = (p or "").strip().upper()
    if p_up in {"PM25", "PM2.5"}:
        return "PM2.5"
    if p_up in {"O3", "OZONE"}:
        return "OZONE"
    return p_up or "UNKNOWN"


def to_api_param_list(param_str: str) -> str:
    mapping = {"PM2.5": "PM25", "PM25": "PM25", "PM10": "PM10", "O3": "OZONE", "OZONE": "OZONE"}
    items = []
    for raw in param_str.split(","):
        key = raw.strip().upper()
        if key:
            items.append(mapping.get(key, key))
    return ",".join(dict.fromkeys(items))


# --------------------------------------------------------------------------
# Stichprobenplan (UTC-Stunden-Snapshots)
# --------------------------------------------------------------------------
def build_sample_slots(
    start_day: date,
    end_day: date,
    fire_months: set[int],
    fire_step: int,
    off_step: int,
    fire_hours: list[int],
    off_hours: list[int],
    density_factor: int = 1,
) -> list[datetime]:
    eff_fire_step = max(1, round(fire_step / density_factor)) if fire_step > 1 else 1
    eff_off_step = max(1, round(off_step / density_factor))

    eff_fire_hours = list(fire_hours)
    if density_factor >= 2 and fire_step == 1 and len(eff_fire_hours) == 1:
        second_hour = (eff_fire_hours[0] + 12) % 24
        eff_fire_hours = sorted([eff_fire_hours[0], second_hour])

    slots = []
    cur = start_day
    fire_idx = 0
    off_idx = 0

    while cur <= end_day:
        if cur.month in fire_months:
            if fire_idx % eff_fire_step == 0:
                for h in sorted(set(eff_fire_hours)):
                    slots.append(datetime(cur.year, cur.month, cur.day, h, 0, tzinfo=timezone.utc))
            fire_idx += 1
            off_idx = 0
        else:
            if off_idx % eff_off_step == 0:
                for h in sorted(set(off_hours)):
                    slots.append(datetime(cur.year, cur.month, cur.day, h, 0, tzinfo=timezone.utc))
            off_idx += 1
            fire_idx = 0
        cur += timedelta(days=1)

    return sorted(slots)


# --------------------------------------------------------------------------
# Rate-Limiter & AirNow API-Abruf
# --------------------------------------------------------------------------
class SlidingRateLimiter:
    def __init__(self, max_per_hour: int = 480, min_delay: float = 0.4):
        self.max_per_hour = max_per_hour
        self.min_delay = min_delay
        self.timestamps: deque[float] = deque()

    def wait(self):
        now = time.time()
        if self.timestamps and (now - self.timestamps[-1]) < self.min_delay:
            time.sleep(self.min_delay - (now - self.timestamps[-1]))
            now = time.time()

        while self.timestamps and (now - self.timestamps[0]) >= 3600.0:
            self.timestamps.popleft()

        if len(self.timestamps) >= self.max_per_hour:
            sleep_sec = 3600.0 - (now - self.timestamps[0]) + 2.0
            print(f"\n[Rate-Limit-Schutz] {self.max_per_hour} Calls/Std. erreicht – pausiere {sleep_sec/60:.1f} Min. ...")
            time.sleep(max(1.0, sleep_sec))

        self.timestamps.append(time.time())


def split_bbox(bbox_str: str, tiles: int) -> list[str]:
    if tiles <= 1:
        return [bbox_str]
    min_lon, min_lat, max_lon, max_lat = map(float, bbox_str.split(","))
    mid_lon = (min_lon + max_lon) / 2.0
    if tiles == 2:
        return [
            f"{min_lon},{min_lat},{mid_lon},{max_lat}",
            f"{mid_lon},{min_lat},{max_lon},{max_lat}",
        ]
    mid_lat = (min_lat + max_lat) / 2.0
    return [
        f"{min_lon},{min_lat},{mid_lon},{mid_lat}",
        f"{mid_lon},{min_lat},{max_lon},{mid_lat}",
        f"{min_lon},{mid_lat},{mid_lon},{max_lat}",
        f"{mid_lon},{mid_lat},{max_lon},{max_lat}",
    ]


def fetch_airnow_snapshot(
    slot_utc: datetime,
    bbox: str,
    api_parameters: str,
    api_key: str,
    limiter: SlidingRateLimiter,
    retries: int = 3,
) -> list[dict]:
    url = "https://www.airnowapi.org/aq/data/"
    ts_str = slot_utc.strftime("%Y-%m-%dT%H")
    params = {
        "startDate": ts_str,
        "endDate": ts_str,
        "parameters": api_parameters,
        "BBOX": bbox,
        "dataType": "B",
        "format": "application/json",
        "verbose": "1",
        "API_KEY": api_key,
    }

    for attempt in range(1, retries + 1):
        limiter.wait()
        try:
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code == 429:
                print(f"\nHTTP 429 Rate Limit bei {ts_str} – warte 65s ...")
                time.sleep(65)
                continue
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and "WebServiceError" in data:
                raise RuntimeError(f"AirNow API Fehler: {data['WebServiceError']}")
            compact = []
            for r in data:
                sid = r.get("FullAQSCode") or r.get("IntlAQSCode") or r.get("SiteName")
                compact.append({
                    "StationID": str(sid),
                    "SiteName": r.get("SiteName"),
                    "AgencyName": r.get("AgencyName"),
                    "CountryCode": r.get("CountryCode"),
                    "Lat": r.get("Latitude"),
                    "Lon": r.get("Longitude"),
                    "Parameter": normalize_param(r.get("Parameter")),
                    "UTC": r.get("UTC"),
                })
            return compact
        except Exception:
            if attempt == retries:
                raise
            time.sleep(2.0 * attempt)
    return []


# --------------------------------------------------------------------------
# Cache & Vorselektion
# --------------------------------------------------------------------------
def load_cache(cache_path: Path, bbox: str, api_parameters: str) -> dict[str, list[dict]]:
    cache = {}
    if not cache_path.exists():
        return cache
    with open(cache_path, encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if obj.get("bbox") == bbox and obj.get("params") == api_parameters:
                cache[obj["slot"]] = obj["records"]
    return cache


def load_preselected_stations(summary_csv: Path, min_avail_pct: float) -> set[str]:
    selected = set()
    with open(summary_csv, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                avail = float(row.get("availability_active_span_pct") or 0.0)
            except ValueError:
                avail = 0.0
            if avail >= min_avail_pct:
                selected.add(str(row["StationID"]))
    return selected


# --------------------------------------------------------------------------
# Auswertung: Erster/Letzter Datenpunkt & Lückenanalyse je Parameter
# --------------------------------------------------------------------------
def analyze_airnow(
    slots: list[datetime],
    cache: dict[str, list[dict]],
    us_only: bool,
    allowed_station_ids: set[str],
    fire_months: set[int],
    fire_step: int,
    off_step: int,
    out_dir: Path,
):
    slot_keys = [s.strftime("%Y-%m-%dT%H") for s in slots]
    slot_to_idx = {k: idx for idx, k in enumerate(slot_keys)}

    meta: dict[tuple[str, str], dict] = {}
    hits: dict[tuple[str, str], set[int]] = {}
    station_params: dict[str, set[str]] = {}

    for s_key in slot_keys:
        idx = slot_to_idx[s_key]
        for rec in cache.get(s_key, []):
            cc = (rec.get("CountryCode") or "").strip().upper()
            sid = str(rec.get("StationID") or "")
            if us_only and cc and cc != "US":
                continue
            if us_only and not cc and sid and not sid.startswith("840") and len(sid) >= 9:
                continue
            if allowed_station_ids and sid not in allowed_station_ids:
                continue

            param = normalize_param(rec.get("Parameter"))
            key = (sid, param)
            station_params.setdefault(sid, set()).add(param)
            if key not in meta:
                meta[key] = {
                    "StationID": sid,
                    "SiteName": rec.get("SiteName"),
                    "AgencyName": rec.get("AgencyName"),
                    "CountryCode": cc or "US",
                    "Lat": rec.get("Lat"),
                    "Lon": rec.get("Lon"),
                    "Parameter": param,
                }
                hits[key] = set()
            hits[key].add(idx)

    if not meta:
        print("Keine Stationen nach Filterung vorhanden.")
        return

    total_slots = len(slots)
    fire_indices = [i for i, dt in enumerate(slots) if dt.month in fire_months]
    summary_rows = []
    gap_rows = []

    for key, info in sorted(meta.items(), key=lambda x: (x[0][1], x[0][0])):
        sid = info["StationID"]
        present_set = hits[key]
        flags = [i in present_set for i in range(total_slots)]
        active_indices = sorted(present_set)
        active_slots = len(active_indices)

        first_idx = active_indices[0]
        last_idx = active_indices[-1]
        first_seen = slots[first_idx].strftime("%Y-%m-%d %H:%M UTC")
        last_seen = slots[last_idx].strftime("%Y-%m-%d %H:%M UTC")

        span_len = last_idx - first_idx + 1
        span_hits = sum(flags[first_idx : last_idx + 1])
        avail_total_pct = round(100.0 * active_slots / total_slots, 1)
        avail_span_pct = round(100.0 * span_hits / span_len, 1)
        fire_hits = sum(1 for i in fire_indices if i in present_set)
        fire_avail_pct = round(100.0 * fire_hits / len(fire_indices), 1) if fire_indices else 0.0

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

            if j < first_idx:
                gap_type = "leading"
            elif i > last_idx:
                gap_type = "trailing"
            else:
                gap_type = "internal"

            step_d = float(fire_step if slots[i].month in fire_months else off_step)
            approx_days = round((slots[j] - slots[i]).total_seconds() / 86400.0 + step_d, 1)
            overlaps_fire = any(slots[k].month in fire_months for k in range(i, j + 1))

            if gap_type == "internal":
                internal_gaps_count += 1
                max_internal_gap_days = max(max_internal_gap_days, approx_days)

            gap_rows.append({
                "StationID": sid,
                "SiteName": info["SiteName"],
                "Parameter": info["Parameter"],
                "gap_type": gap_type,
                "gap_start_utc": slots[i].strftime("%Y-%m-%d %H:%M"),
                "gap_end_utc": slots[j].strftime("%Y-%m-%d %H:%M"),
                "missing_samples": j - i + 1,
                "approx_gap_days": approx_days,
                "overlaps_fire_season": overlaps_fire,
            })
            i = j + 1

        summary_rows.append({
            **info,
            "station_all_parameters": "|".join(sorted(station_params.get(sid, set()))),
            "first_seen_utc": first_seen,
            "last_seen_utc": last_seen,
            "total_samples": total_slots,
            "active_samples": active_slots,
            "availability_total_pct": avail_total_pct,
            "availability_active_span_pct": avail_span_pct,
            "fire_season_avail_pct": fire_avail_pct,
            "internal_gaps_count": internal_gaps_count,
            "max_internal_gap_days": max_internal_gap_days,
        })

    df_sum = pd.DataFrame(summary_rows).sort_values(["Parameter", "active_samples"], ascending=[True, False])
    sum_path = out_dir / "airnow_conus_stations_summary.csv"
    df_sum.to_csv(sum_path, index=False)

    gap_path = out_dir / "airnow_conus_gaps.csv"
    if gap_rows:
        pd.DataFrame(gap_rows).to_csv(gap_path, index=False)

    print(f"\n=== AirNow CONUS-Zusammenfassung ({total_slots} Stichproben-Zeitpunkte) ===")
    for param, grp in df_sum.groupby("Parameter"):
        high_qual = int((grp["availability_active_span_pct"] >= 80.0).sum())
        print(f"  Parameter '{param}': {len(grp)} Stationen aktiv, davon {high_qual} mit >=80% Verfügbarkeit (aktive Spanne)")
    print(f"\nGespeichert:\n  - {sum_path}\n  - {gap_path}")


# --------------------------------------------------------------------------
# Hauptprogramm
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="CONUS Vorselektion für AirNow (PM2.5, PM10, OZONE)")
    ap.add_argument("--start", default="2024-01-01", help="Startdatum (YYYY-MM-DD, Standard: 2024-01-01)")
    ap.add_argument("--end", default=date.today().isoformat(), help="Enddatum (YYYY-MM-DD, Standard: heute)")
    ap.add_argument("--bbox", default=CONUS_BBOX, help="Bounding Box minLon,minLat,maxLon,maxLat")
    ap.add_argument("--tiles", type=int, choices=[1, 2, 4], default=1,
                    help="CONUS in 1, 2 oder 4 Kacheln aufteilen (Standard: 1)")
    ap.add_argument("--parameters", default="PM25,PM10,OZONE",
                    help="Kommagetrennte Schadstoffe (Standard: PM25,PM10,OZONE)")
    ap.add_argument("--fire-months", nargs="+", type=int, default=[5, 6, 7, 8, 9, 10],
                    help="Waldbrandmonate 1..12 (Standard: 5 6 7 8 9 10)")
    ap.add_argument("--fire-step", type=int, default=1, help="Schrittweite in Tagen (Waldbrandmonate, Standard: 1)")
    ap.add_argument("--off-step", type=int, default=3, help="Schrittweite in Tagen (Nebensaison, Standard: 3)")
    ap.add_argument("--fire-hours", nargs="+", type=int, default=[14],
                    help="UTC-Stunde(n) in Waldbrandmonaten (Standard: 14 = 07:00 PDT)")
    ap.add_argument("--off-hours", nargs="+", type=int, default=[14],
                    help="UTC-Stunde(n) in Nebensaison (Standard: 14)")
    ap.add_argument("--dense", action="store_true", help="Verdichteter Lauf (entspricht --density-factor 2)")
    ap.add_argument("--density-factor", type=int, default=1, help="Verdichtungsfaktor (z. B. 2)")
    ap.add_argument("--station-ids", nargs="+", help="Nur diese StationIDs/AQS-Codes auswerten")
    ap.add_argument("--from-summary", type=Path, help="Vorselektion aus Summary-CSV laden")
    ap.add_argument("--min-avail", type=float, default=50.0,
                    help="Mindestverfügbarkeit (%%) bei --from-summary (Standard: 50.0)")
    ap.add_argument("--include-non-us", action="store_true", help="Auch Grenzstationen in Kanada/Mexiko behalten")
    ap.add_argument("--max-calls-per-hour", type=int, default=480, help="Rate-Limit Puffer (Standard: 480/Std.)")
    ap.add_argument("--stop-after", type=int, default=0,
                    help="Optional: Nach N API-Calls sauber beenden (0 = durchlaufen)")
    ap.add_argument("--api-key", default=DEFAULT_API_KEY, help="AirNow API Key")
    ap.add_argument("--out", default="airnow_conus_check", help="Ausgabeordner")
    ap.add_argument("--fresh", action="store_true", help="Cache löschen")
    args = ap.parse_args()

    api_params = to_api_param_list(args.parameters)
    factor = max(args.density_factor, 2 if args.dense else 1)
    start_day = date.fromisoformat(args.start)
    end_day = min(date.fromisoformat(args.end), date.today())
    fire_months = set(args.fire_months)

    slots = build_sample_slots(
        start_day, end_day, fire_months,
        args.fire_step, args.off_step, args.fire_hours, args.off_hours, factor
    )
    now_utc = datetime.now(timezone.utc)
    slots = [s for s in slots if s <= now_utc]

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_path = out_dir / "airnow_conus_cache.jsonl"
    if args.fresh and cache_path.exists():
        cache_path.unlink()

    cache = load_cache(cache_path, args.bbox, api_params)
    pending = [s for s in slots if s.strftime("%Y-%m-%dT%H") not in cache]

    print(f"AirNow Stichprobenplan ({api_params}): {len(slots)} Zeitpunkte ({start_day} bis {end_day}, Faktor={factor}x)")
    print(f"  Bereits im Cache: {len(slots) - len(pending)} | Offen: {len(pending)} (Kacheln pro Call: {args.tiles})")

    limiter = SlidingRateLimiter(max_per_hour=args.max_calls_per_hour)
    bboxes = split_bbox(args.bbox, args.tiles)
    calls_made = 0

    if pending:
        with open(cache_path, "a", encoding="utf-8") as cf:
            for idx, slot in enumerate(pending, 1):
                s_key = slot.strftime("%Y-%m-%dT%H")
                merged_records = []
                for b in bboxes:
                    recs = fetch_airnow_snapshot(slot, b, api_params, args.api_key, limiter)
                    merged_records.extend(recs)
                    calls_made += 1

                cache[s_key] = merged_records
                cf.write(json.dumps({
                    "slot": s_key,
                    "bbox": args.bbox,
                    "params": api_params,
                    "records": merged_records,
                }) + "\n")
                cf.flush()

                if idx % 20 == 0 or idx == len(pending):
                    print(f"  [{idx}/{len(pending)}] {s_key} UTC -> {len(merged_records)} Datensätze (API-Calls: {calls_made})")

                if args.stop_after and calls_made >= args.stop_after:
                    print(f"\nStopp nach {calls_made} API-Calls (--stop-after). Zum Fortsetzen denselben Befehl erneut starten.")
                    break

    allowed_ids = set(args.station_ids or [])
    if args.from_summary:
        allowed_ids |= load_preselected_stations(args.from_summary, args.min_avail)

    eff_fire_step = max(1, round(args.fire_step / factor)) if args.fire_step > 1 else 1
    eff_off_step = max(1, round(args.off_step / factor))

    analyze_airnow(
        slots, cache, us_only=not args.include_non_us,
        allowed_station_ids=allowed_ids,
        fire_months=fire_months,
        fire_step=eff_fire_step,
        off_step=eff_off_step,
        out_dir=out_dir,
    )


if __name__ == "__main__":
    main()