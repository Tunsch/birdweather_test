#!/usr/bin/env python3
"""
Räumliches und zeitliches Matching zwischen vorselektierten BirdWeather-PUC-Stationen
(aqi, eco2, tvoc) und AirNow-Referenzstationen (PM2.5, PM10, OZONE) in CONUS.
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def haversine_matrix_km(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    """Berechnet die paarweise Haversine-Distanz in km (Shape: [len(lat1), len(lat2)])."""
    r_earth_km = 6371.0088
    phi1 = np.radians(lat1)[:, None]
    phi2 = np.radians(lat2)[None, :]
    dphi = phi2 - phi1
    dlambda = np.radians(lon2)[None, :] - np.radians(lon1)[:, None]

    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * (np.sin(dlambda / 2.0) ** 2)
    return 2.0 * r_earth_km * np.arcsin(np.minimum(1.0, np.sqrt(a)))


def prepare_puc_table(puc_csv: Path, min_avail_pct: float) -> pd.DataFrame:
    df = pd.read_csv(puc_csv)
    df = df.dropna(subset=["lat", "lon"]).copy()
    df["id"] = df["id"].astype(str)
    df["first_dt"] = pd.to_datetime(df["first_seen"], errors="coerce", utc=True)
    df["last_dt"] = pd.to_datetime(df["last_seen"], errors="coerce", utc=True)

    # Verfügbarkeit je PUC-Variable pivotieren
    piv_avail = df.pivot_table(
        index="id", columns="sensor", values="availability_active_span_pct", aggfunc="max"
    ).fillna(0.0)
    piv_fire = df.pivot_table(
        index="id", columns="sensor", values="fire_season_avail_pct", aggfunc="max"
    ).fillna(0.0)

    meta = (
        df[df["active_samples"] > 0]
        .groupby("id", as_index=False)
        .agg(
            puc_name=("name", "first"),
            puc_state=("state", "first"),
            puc_lat=("lat", "first"),
            puc_lon=("lon", "first"),
            puc_first_seen=("first_dt", "min"),
            puc_last_seen=("last_dt", "max"),
            puc_max_internal_gap_days=("max_internal_gap_days", "max"),
        )
    )

    for var in ["aqi", "eco2", "tvoc"]:
        meta[f"puc_{var}_avail_pct"] = meta["id"].map(piv_avail[var] if var in piv_avail else {}).fillna(0.0)
        meta[f"puc_{var}_fire_pct"] = meta["id"].map(piv_fire[var] if var in piv_fire else {}).fillna(0.0)

    meta["puc_best_var_avail_pct"] = meta[["puc_aqi_avail_pct", "puc_eco2_avail_pct", "puc_tvoc_avail_pct"]].max(axis=1)
    meta = meta[meta["puc_best_var_avail_pct"] >= min_avail_pct].copy()
    return meta.reset_index(drop=True)


def prepare_airnow_table(airnow_csv: Path, min_avail_pct: float) -> pd.DataFrame:
    df = pd.read_csv(airnow_csv)
    df = df.dropna(subset=["Lat", "Lon"]).copy()
    df["StationID"] = df["StationID"].astype(str)
    df["first_dt"] = pd.to_datetime(df["first_seen_utc"], errors="coerce", utc=True)
    df["last_dt"] = pd.to_datetime(df["last_seen_utc"], errors="coerce", utc=True)

    # Nur Parameter behalten, die die Mindestverfügbarkeit erfüllen
    df_qual = df[df["availability_active_span_pct"] >= min_avail_pct].copy()
    if df_qual.empty:
        return pd.DataFrame()

    piv_avail = df_qual.pivot_table(
        index="StationID", columns="Parameter", values="availability_active_span_pct", aggfunc="max"
    ).fillna(0.0)
    piv_fire = df_qual.pivot_table(
        index="StationID", columns="Parameter", values="fire_season_avail_pct", aggfunc="max"
    ).fillna(0.0)

    params_per_station = (
        df_qual.groupby("StationID")["Parameter"]
        .apply(lambda s: "|".join(sorted(set(s))))
        .to_dict()
    )

    meta = (
        df_qual.groupby("StationID", as_index=False)
        .agg(
            airnow_name=("SiteName", "first"),
            airnow_agency=("AgencyName", "first"),
            airnow_lat=("Lat", "first"),
            airnow_lon=("Lon", "first"),
            airnow_first_seen=("first_dt", "min"),
            airnow_last_seen=("last_dt", "max"),
            airnow_max_internal_gap_days=("max_internal_gap_days", "max"),
        )
    )
    meta["airnow_parameters"] = meta["StationID"].map(params_per_station)

    col_map = {"PM2.5": "pm25", "PM10": "pm10", "OZONE": "ozone"}
    for raw_p, short_p in col_map.items():
        meta[f"airnow_{short_p}_avail_pct"] = meta["StationID"].map(piv_avail[raw_p] if raw_p in piv_avail else {}).fillna(0.0)
        meta[f"airnow_{short_p}_fire_pct"] = meta["StationID"].map(piv_fire[raw_p] if raw_p in piv_fire else {}).fillna(0.0)

    return meta.reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser(description="Räumliches Matching BirdWeather PUC <-> AirNow CONUS")
    ap.add_argument("--puc-summary", type=Path,
                    default=Path("birdweather_conus_check/birdweather_conus_stations_summary.csv"))
    ap.add_argument("--airnow-summary", type=Path,
                    default=Path("airnow_conus_check/airnow_conus_stations_summary.csv"))
    ap.add_argument("--max-dist-km", type=float, default=50.0, help="Maximaler Suchradius in km (Standard: 50.0)")
    ap.add_argument("--top-k", type=int, default=5, help="Max. Anzahl AirNow-Nachbarn je PUC (Standard: 5)")
    ap.add_argument("--min-puc-avail", type=float, default=50.0,
                    help="Mindestverfügbarkeit (%%) der PUC-Station (Standard: 50.0)")
    ap.add_argument("--min-airnow-avail", type=float, default=50.0,
                    help="Mindestverfügbarkeit (%%) der AirNow-Station (Standard: 50.0)")
    ap.add_argument("--min-overlap-days", type=int, default=14,
                    help="Mindestüberlappung der aktiven Betriebszeiträume in Tagen (Standard: 14)")
    ap.add_argument("--out", type=Path, default=Path("matched_stations"), help="Ausgabeordner")
    args = ap.parse_args()

    puc = prepare_puc_table(args.puc_summary, args.min_puc_avail)
    airnow = prepare_airnow_table(args.airnow_summary, args.min_airnow_avail)

    if puc.empty or airnow.empty:
        raise SystemExit(f"Zu wenig Stationen nach Filterung (PUC: {len(puc)}, AirNow: {len(airnow)}).")

    print(f"Starte Matching: {len(puc)} PUC-Stationen x {len(airnow)} AirNow-Stationen (Radius <= {args.max_dist_km} km) ...")
    dist_mat = haversine_matrix_km(
        puc["puc_lat"].to_numpy(), puc["puc_lon"].to_numpy(),
        airnow["airnow_lat"].to_numpy(), airnow["airnow_lon"].to_numpy(),
    )

    pair_rows = []
    best_rows = []

    for i, p_row in puc.iterrows():
        dists = dist_mat[i]
        cand_idx = np.where(dists <= args.max_dist_km)[0]
        if len(cand_idx) == 0:
            continue
        cand_idx = cand_idx[np.argsort(dists[cand_idx])]

        valid_cands = []
        for j in cand_idx:
            a_row = airnow.iloc[j]
            ov_start = max(p_row["puc_first_seen"], a_row["airnow_first_seen"])
            ov_end = min(p_row["puc_last_seen"], a_row["airnow_last_seen"])
            ov_days = int((ov_end - ov_start).total_seconds() // 86400) if pd.notna(ov_start) and pd.notna(ov_end) else 0
            if ov_days < args.min_overlap_days:
                continue
            valid_cands.append((j, float(dists[j]), ov_start, ov_end, ov_days))

        if not valid_cands:
            continue

        # Nächste Station je Schadstoff bestimmen
        nearest_pm25_id = next((airnow.iloc[j]["StationID"] for j, *_ in valid_cands if airnow.iloc[j]["airnow_pm25_avail_pct"] > 0), None)
        nearest_pm10_id = next((airnow.iloc[j]["StationID"] for j, *_ in valid_cands if airnow.iloc[j]["airnow_pm10_avail_pct"] > 0), None)
        nearest_ozone_id = next((airnow.iloc[j]["StationID"] for j, *_ in valid_cands if airnow.iloc[j]["airnow_ozone_avail_pct"] > 0), None)

        for rank, (j, d_km, ov_start, ov_end, ov_days) in enumerate(valid_cands[: args.top_k], start=1):
            a_row = airnow.iloc[j]
            pair_rows.append({
                "puc_id": p_row["id"],
                "puc_name": p_row["puc_name"],
                "puc_state": p_row["puc_state"],
                "puc_lat": p_row["puc_lat"],
                "puc_lon": p_row["puc_lon"],
                "puc_aqi_avail_pct": p_row["puc_aqi_avail_pct"],
                "puc_eco2_avail_pct": p_row["puc_eco2_avail_pct"],
                "puc_tvoc_avail_pct": p_row["puc_tvoc_avail_pct"],
                "puc_aqi_fire_pct": p_row["puc_aqi_fire_pct"],
                "neighbor_rank": rank,
                "distance_km": round(d_km, 2),
                "overlap_start": ov_start.strftime("%Y-%m-%d"),
                "overlap_end": ov_end.strftime("%Y-%m-%d"),
                "overlap_days": ov_days,
                "airnow_id": a_row["StationID"],
                "airnow_name": a_row["airnow_name"],
                "airnow_agency": a_row["airnow_agency"],
                "airnow_lat": a_row["airnow_lat"],
                "airnow_lon": a_row["airnow_lon"],
                "airnow_parameters": a_row["airnow_parameters"],
                "airnow_pm25_avail_pct": a_row["airnow_pm25_avail_pct"],
                "airnow_pm10_avail_pct": a_row["airnow_pm10_avail_pct"],
                "airnow_ozone_avail_pct": a_row["airnow_ozone_avail_pct"],
                "is_nearest_pm25": a_row["StationID"] == nearest_pm25_id,
                "is_nearest_pm10": a_row["StationID"] == nearest_pm10_id,
                "is_nearest_ozone": a_row["StationID"] == nearest_ozone_id,
            })

        # Kompakte Best-Pair-Zeile pro PUC-Station
        j0, d0, ov_s0, ov_e0, ov_d0 = valid_cands[0]
        a0 = airnow.iloc[j0]
        pm25_hit = next(((airnow.iloc[j], d) for j, d, *_ in valid_cands if airnow.iloc[j]["airnow_pm25_avail_pct"] > 0), (None, None))
        pm10_hit = next(((airnow.iloc[j], d) for j, d, *_ in valid_cands if airnow.iloc[j]["airnow_pm10_avail_pct"] > 0), (None, None))
        o3_hit = next(((airnow.iloc[j], d) for j, d, *_ in valid_cands if airnow.iloc[j]["airnow_ozone_avail_pct"] > 0), (None, None))

        best_rows.append({
            "puc_id": p_row["id"],
            "puc_name": p_row["puc_name"],
            "puc_state": p_row["puc_state"],
            "puc_lat": p_row["puc_lat"],
            "puc_lon": p_row["puc_lon"],
            "puc_aqi_avail_pct": p_row["puc_aqi_avail_pct"],
            "puc_eco2_avail_pct": p_row["puc_eco2_avail_pct"],
            "puc_tvoc_avail_pct": p_row["puc_tvoc_avail_pct"],
            "nearest_airnow_id": a0["StationID"],
            "nearest_airnow_name": a0["airnow_name"],
            "nearest_dist_km": round(d0, 2),
            "nearest_params": a0["airnow_parameters"],
            "nearest_overlap_days": ov_d0,
            "nearest_pm25_id": pm25_hit[0]["StationID"] if pm25_hit[0] is not None else "",
            "nearest_pm25_dist_km": round(pm25_hit[1], 2) if pm25_hit[1] is not None else "",
            "nearest_pm10_id": pm10_hit[0]["StationID"] if pm10_hit[0] is not None else "",
            "nearest_pm10_dist_km": round(pm10_hit[1], 2) if pm10_hit[1] is not None else "",
            "nearest_ozone_id": o3_hit[0]["StationID"] if o3_hit[0] is not None else "",
            "nearest_ozone_dist_km": round(o3_hit[1], 2) if o3_hit[1] is not None else "",
        })

    args.out.mkdir(parents=True, exist_ok=True)
    if not pair_rows:
        print("Keine Paare gefunden, die Radius- und Überlappungskriterien erfüllen.")
        return

    df_pairs = pd.DataFrame(pair_rows).sort_values(["distance_km", "puc_id", "neighbor_rank"])
    df_best = pd.DataFrame(best_rows).sort_values("nearest_dist_km")

    pairs_path = args.out / "puc_airnow_matches.csv"
    best_path = args.out / "puc_airnow_best_pairs.csv"
    df_pairs.to_csv(pairs_path, index=False)
    df_best.to_csv(best_path, index=False)

    print(f"\n=== Matching-Ergebnis ===")
    print(f"  Gematchete PUC-Stationen (<= {args.max_dist_km} km, >= {args.min_overlap_days}d Überlappung): {len(df_best)}")
    print(f"  Davon <= 10 km zur nächsten AirNow-Station: {int((df_best['nearest_dist_km'] <= 10.0).sum())}")
    print(f"  Davon <= 25 km zur nächsten AirNow-Station: {int((df_best['nearest_dist_km'] <= 25.0).sum())}")
    print(f"\nGespeichert:\n  - {best_path}\n  - {pairs_path}")


if __name__ == "__main__":
    main()