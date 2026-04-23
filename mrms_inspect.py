from pathlib import Path
import re
import numpy as np
import pandas as pd

base_path = Path("/ncrc/home2/Anthony.Preucil/apscratch/EF5_work/Forcings/Precipitation/hourly")
output_dir = Path.cwd() / "mrms_completeness_output"
output_dir.mkdir(exist_ok=True)

if not base_path.exists():
    raise FileNotFoundError(f"Base path does not exist: {base_path}")

def extract_timestamp(text):
    pattern, parser = (re.compile(r"(?<!\d)(\d{8})[_-](\d{6})(?!\d)"), lambda m: pd.to_datetime("".join(m.groups()), format="%Y%m%d%H%M%S", errors="coerce"))

    match = pattern.search(text)
    if match:
        ts = parser(match)
        if pd.notna(ts):
            return ts.floor("h")

    return pd.NaT


def infer_region(path_obj, root):
    rel = path_obj.relative_to(root)
    # Region is always the first subfolder under the base folder.
    return rel.parts[0] if len(rel.parts) > 1 else "root"


def longest_missing_streak_hours(missing_index):
    if len(missing_index) == 0:
        return 0

    s = pd.Series(missing_index).sort_values().reset_index(drop=True)
    groups = (s.diff() != pd.Timedelta(hours=1)).cumsum()
    return int(s.groupby(groups).size().max())


all_files = sorted(p for p in base_path.rglob("*") if p.is_file() and not p.name.startswith("."))

records = []
unparsed_files = []

for path_obj in all_files:
    ts = extract_timestamp(path_obj.name)
    if pd.isna(ts):
        unparsed_files.append(str(path_obj))
        continue

    records.append(
        {
            "path": str(path_obj),
            "region": infer_region(path_obj, base_path),
            "timestamp": ts,
        }
    )

if not records:
    raise ValueError("No files with parseable timestamps were found.")

files_df = pd.DataFrame(records).sort_values(["region", "timestamp", "path"]).reset_index(drop=True)

summary_rows = []
missing_rows = []
missing_timesteps_by_region = {}

for region, group in files_df.groupby("region"):
    timestamps = pd.DatetimeIndex(sorted(group["timestamp"].unique()))
    expected = pd.date_range(timestamps.min(), timestamps.max(), freq="h")
    missing = expected.difference(timestamps)

    files_count = len(group)
    unique_hours = len(timestamps)
    duplicate_files = files_count - unique_hours
    expected_hours = len(expected)
    missing_hours = len(missing)
    completeness_pct = 100 * unique_hours / expected_hours if expected_hours else np.nan

    summary_rows.append(
        {
            "region": region,
            "start": timestamps.min(),
            "end": timestamps.max(),
            "files_found": files_count,
            "unique_hours_found": unique_hours,
            "duplicate_files": duplicate_files,
            "expected_hours": expected_hours,
            "missing_hours": missing_hours,
            "completeness_pct": round(completeness_pct, 2),
            "longest_missing_streak_h": longest_missing_streak_hours(missing),
        }
    )

    missing_timesteps_by_region[region] = list(missing)

    for ts in missing:
        missing_rows.append({"region": region, "missing_timestep": ts})

summary_df = pd.DataFrame(summary_rows).sort_values(["completeness_pct", "region"]).reset_index(drop=True)
missing_df = pd.DataFrame(missing_rows).sort_values(["region", "missing_timestep"]).reset_index(drop=True)

summary_df.to_csv(output_dir / "region_completeness_summary.csv", index=False)
missing_df.to_csv(output_dir / "missing_timesteps_by_region.csv", index=False)

print(f"Scanned files: {len(all_files):,}")
print(f"Files with parseable timestamps: {len(files_df):,}")
print(f"Files with unparseable timestamps: {len(unparsed_files):,}")
print(f"Results written to: {output_dir}")