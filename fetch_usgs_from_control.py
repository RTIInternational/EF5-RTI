#!/usr/bin/env python3
import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

CFS_TO_CMS = 0.028316846592


def parse_ef5_time(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


def parse_cli_time(value: str) -> datetime:
    value = value.strip()

    try:
        return parse_ef5_time(value)
    except ValueError:
        pass

    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except ValueError:
        pass

    try:
        dt = datetime.strptime(value, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(
            f"Invalid date/time '{value}'. Supported formats: YYYYMMDDHHMMSS, YYYY-MM-DD, or ISO-8601."
        ) from exc


def fetch_usgs_iv(gage_id: str, start_utc: datetime, end_utc: datetime):
    params = {
        "format": "json",
        "sites": gage_id,
        "parameterCd": "00060",
        "startDT": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endDT": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "siteStatus": "all",
    }
    url = "https://waterservices.usgs.gov/nwis/iv/?" + urlencode(params)

    with urlopen(url) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    series_list = payload.get("value", {}).get("timeSeries", [])
    if not series_list:
        return []

    values = series_list[0].get("values", [])
    if not values:
        return []

    points = []
    for row in values[0].get("value", []):
        dt_str = row.get("dateTime", "")
        val_str = row.get("value", "")
        if not dt_str or val_str in ("", None):
            continue

        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)

        try:
            q_cfs = float(val_str)
        except ValueError:
            continue

        points.append((dt, q_cfs * CFS_TO_CMS))

    points.sort(key=lambda x: x[0])
    return points


def write_csv(rows, out_csv: Path):
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Date (UTC)", "Q (cms)"])
        for dt, q in rows:
            w.writerow([dt.strftime("%Y-%m-%d %H:%M:%S"), f"{q:.6f}"])


def main():
    parser = argparse.ArgumentParser(
        description="Download USGS streamflow and write CSV in cms."
    )
    parser.add_argument(
        "--gauge",
        required=True,
        help="USGS gauge/site id (example: 04085200)",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start datetime in UTC. Supported: YYYYMMDDHHMMSS, YYYY-MM-DD, ISO-8601",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End datetime in UTC. Supported: YYYYMMDDHHMMSS, YYYY-MM-DD, ISO-8601",
    )
    parser.add_argument(
        "--outdir",
        default=str(Path.home() / "Kewaunee" / "observations"),
        help="Output directory for CSV",
    )

    args = parser.parse_args()

    outdir = Path(args.outdir).expanduser().resolve()

    gauge_id = args.gauge.strip()
    start_utc = parse_cli_time(args.start_date)
    end_utc = parse_cli_time(args.end_date)

    if end_utc <= start_utc:
        raise ValueError("--end-date must be after --start-date")
    
    raw_points = fetch_usgs_iv(gauge_id, start_utc, end_utc)
    if not raw_points:
        raise RuntimeError("No USGS streamflow data returned for the requested range and gauge.")

    rows = [(dt, q) for dt, q in raw_points if start_utc <= dt <= end_utc]

    out_csv = outdir / f"Streamflow_Time_Series_CMS_UTC_USGS_{gauge_id}.csv"
    write_csv(rows, out_csv)

    print(f"Gauge: {gauge_id}")
    print(f"TIME_BEGIN: {start_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"TIME_END:   {end_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("TIMESTEP:   all available timesteps (raw USGS IV)")
    print(f"Rows written: {len(rows)}")
    print(f"Output: {out_csv}")


if __name__ == "__main__":
    main()
