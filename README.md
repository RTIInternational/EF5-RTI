# EF5-RTI: EF5-FLASH Input Preparation Toolkit

This repository contains helper scripts and a guided notebook for preparing hydrologic model inputs for EF5-style flash flood simulation workflows.

The core intent is to:
- pull forcing/observation data (MRMS precipitation and USGS streamflow),
- convert and organize files into model-ready formats, and
- run a repeatable, notebook-driven preprocessing workflow.

## Background: EF5-FLASH (high-level)

EF5 is a distributed hydrologic modeling framework commonly used for event-based and real-time flood simulation. In many operational or research workflows, EF5 is paired with high-frequency precipitation products (such as MRMS) and streamflow observations (such as USGS NWIS Instantaneous Values) to support setup, calibration, and evaluation tasks.

In that context, this project focuses on the *data preparation* side of an EF5-FLASH-style workflow:
- preparing precipitation forcing inputs from MRMS PrecipRate archives,
- preparing observed streamflow time series for comparison,
- and walking through raster/model input preparation in the notebook.

## Repository layout

- `prepare_model.ipynb`
  - Primary workflow notebook.
  - Contains the step-by-step process to prepare model inputs (raster clipping, conversion, and related preprocessing tasks).
  - **Recommended path:** follow this notebook sequentially from top to bottom.

- `download_mrms_preciprate.sh`
  - Bash helper script to download MRMS precipitation `.gz` files for a date range.
  - Supports both 2-minute and hourly products for CONUS, Hawaii, Puerto Rico, and Alaska.
  - Uses IEM mtarchive for CONUS and NOAA MRMS PDS (AWS S3) for HI/PR/AK.
  - Requires explicit product selection (`--product 2min` or `--product hourly`).
  - Supports optional region selection (`--region CONUS|HI|PR|AK|all`, default: `all`).
  - Supports dry-run mode, parallel downloads, and skipping files that already exist locally.
  - Optionally decompresses downloaded `.gz` files at the end of a real run.

- `fetch_usgs_from_control.py`
  - Python helper script to download USGS NWIS instantaneous streamflow (`parameterCd=00060`) for one gauge and date range.
  - Converts discharge from cfs to cms and writes a CSV.
  - Exports **all available timesteps** returned by USGS in the requested interval.

- `requirements.txt`
  - Frozen Python package list exported from the working environment used for this project.
  - Use this file to recreate a compatible environment for notebook and script execution.

- `control_files/`
  - Example control/configuration files used for EF5 execution contexts.
  - Useful as templates/reference when connecting prepared inputs into an EF5 run.

- `__pycache__/`
  - Python bytecode cache artifacts.

## Recommended workflow (important)

1. Start with `prepare_model.ipynb` and follow cells in order.
2. Use helper scripts to fetch raw forcing/observation data.
3. Return to notebook steps for conversion/formatting and final model input preparation.

The notebook is the orchestrator for the full pipeline; the scripts are supporting utilities.

## Helper script usage

### 1) Download MRMS precipitation

Script: `download_mrms_preciprate.sh`

Make executable (one-time):

```bash
chmod +x download_mrms_preciprate.sh
```

Show help:

```bash
./download_mrms_preciprate.sh --help
```

Dry-run first (recommended):

```bash
./download_mrms_preciprate.sh \
  --product 2min \
  --region CONUS \
  --start-date 2022-07-27 \
  --end-date 2022-07-30 \
  --dest-dir ~/MRMS_preciprate \
  --dry-run
```

Run actual 2-minute PrecipRate download:

```bash
./download_mrms_preciprate.sh \
  --product 2min \
  --region CONUS \
  --start-date 2022-07-27 \
  --end-date 2022-07-30 \
  --dest-dir ~/MRMS_preciprate
```

Run hourly QPE download for Hawaii (on-the-hour files only):

```bash
./download_mrms_preciprate.sh \
  --product hourly \
  --region HI \
  --start-date 2021-01-01 \
  --end-date 2021-01-02 \
  --dest-dir ~/MRMS_preciprate \
  --jobs 8
```

Run all regions (default) in one command:

```bash
./download_mrms_preciprate.sh \
  --product hourly \
  --start-date 2021-01-01 \
  --end-date 2021-01-02 \
  --dest-dir ~/MRMS_preciprate \
  --jobs 16
```

Behavior notes:
- `--product` is required and must be either `2min` or `hourly`.
- `--start-date` and `--end-date` are required (no default date range).
- `--region` accepts `CONUS`, `HI`, `PR`, `AK`, or `all` (default).
- Source selection by region:
  - `CONUS` -> `https://mtarchive.geol.iastate.edu/.../mrms/ncep/{PrecipRate|RadarOnly_QPE_01H}/`
  - `HI`/`PR`/`AK` -> `https://noaa-mrms-pds.s3.amazonaws.com/{HAWAII|CARIB|ALASKA}/{PrecipRate_00.00|RadarOnly_QPE_01H_00.00}/YYYYMMDD/`
- For `HI`, `PR`, and `AK`, `--start-date` must be `2020-10-15` or later.
- Creates destination directory if it does not exist and writes into product and region subfolders:
  - `--dest-dir ~/MRMS_preciprate` + `--product 2min` + `--region CONUS` -> `~/MRMS_preciprate/2min/CONUS`
  - `--dest-dir ~/MRMS_preciprate` + `--product hourly` + `--region HI` -> `~/MRMS_preciprate/hourly/HI`
- Hourly mode keeps only files at minute/second `00:00` (one file per hour).
- Parallelism is supported with `--jobs N` (defaults to available CPU cores).
- Skips files that already exist (either `.gz` or already decompressed version).
- Downloads are processed incrementally by region and day (not one huge queue), which is better for long multi-year runs.
- Downloads use resume mode (`wget --continue`), so rerunning after interruption will continue partial `.gz` files when the source supports byte ranges.
- Prints a compact summary with skipped/downloaded counts.
- In dry-run mode, no files are downloaded or decompressed.

### 2) Download USGS streamflow observations

Script: `fetch_usgs_from_control.py`

Show help:

```bash
python3 fetch_usgs_from_control.py --help
```

Example:

```bash
python3 fetch_usgs_from_control.py \
  --gauge 04085200 \
  --start-date 2022-07-27 \
  --end-date 2022-07-30 \
  --outdir ~/Kewaunee/observations
```

Behavior notes:
- Accepted date formats: `YYYYMMDDHHMMSS`, `YYYY-MM-DD`, or ISO-8601.
- Output is UTC and includes all available USGS timesteps in the requested interval.
- Output file pattern:
  - `Streamflow_Time_Series_CMS_UTC_USGS_<gauge>.csv`

## Storage projections

The following estimates are based on measured average file sizes from local data
folders and can be used for rough planning.

### Hourly MRMS files (`.grib2.gz`)

Measured basis:
- average hourly file size: 617.43 KB (632,252.82 bytes)
- cadence: 24 files/day

| Interval | Hourly files | Projected storage |
|---|---:|---:|
| 1 day | 24 | 14.47 MB |
| 1 week | 168 | 101.30 MB |
| 1 month (30 days) | 720 | 434.13 MB |
| 1 year (365 days) | 8,760 | 5.16 GB |
| 5 years | 43,800 | 25.79 GB |

### 2-minute MRMS PrecipRate files (`.grib2`)

Measured basis:
- average 2-minute GRIB2 file size: 643.94 KB (659,392 bytes)
- average `.idx` size: 1.33 KB (1,360 bytes)
- cadence: 720 files/day (one file every 2 minutes)

| Interval | 2-minute files | GRIB2 only | GRIB2 + IDX |
|---|---:|---:|---:|
| 1 day | 720 | 452.77 MB | 453.70 MB |
| 1 week | 5,040 | 3.10 GB | 3.10 GB |
| 1 month (30 days) | 21,600 | 13.26 GB | 13.29 GB |
| 1 year (365 days) | 262,800 | 161.39 GB | 161.72 GB |
| 5 years | 1,314,000 | 806.94 GB | 808.60 GB |

## Dependencies and environment

- `download_mrms_preciprate.sh` requires common shell tools and `wget`, `grep`, `sed`, `gunzip`, `xargs`.
- `fetch_usgs_from_control.py` uses Python 3 standard library only.
- Notebook and geospatial preprocessing steps rely on additional Python packages listed in `requirements.txt`.

### Create and install the Python environment

From the `EF5-RTI` repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Optional (for Jupyter notebook kernel selection):

```bash
python -m ipykernel install --user --name ef5-rti --display-name "Python (ef5-rti)"
```

Then open `prepare_model.ipynb` and select the installed `Python (ef5-rti)` kernel.

## Resources

The notebook references the following external sources.

| Source (webpage/repo) | What it is and how it is used |
|---|---|
| [IEM MTArchive (MRMS Precipitation Archives)](https://mtarchive.geol.iastate.edu/) | Iowa State IEM archive hosting historical MRMS precipitation products, used by `download_mrms_preciprate.sh` for CONUS downloads (`PrecipRate` and `RadarOnly_QPE_01H`). |
| [NOAA MRMS PDS on AWS S3](https://noaa-mrms-pds.s3.amazonaws.com/index.html) | Public AWS-hosted MRMS archive used by `download_mrms_preciprate.sh` for HI/PR/AK downloads from `HAWAII`, `CARIB`, and `ALASKA` nests (`PrecipRate_00.00` and `RadarOnly_QPE_01H_00.00`). |
| [USGS StreamStats](https://streamstats.usgs.gov/) | USGS watershed delineation and basin data portal used to obtain basin boundaries and related geospatial inputs for model setup. |
| [USGS WaterData station 04085200](https://waterdata.usgs.gov/monitoring-location/04085200/) | Station information page for the example gage in this workflow, used to verify gauge metadata and context for observation downloads. |
| [HyDROSLab/EF5-US-Parameters](https://github.com/HyDROSLab/EF5-US-Parameters) | Parameter dataset repository referenced for national-scale EF5 parameter layers used as source inputs before clipping/preprocessing. |
| [HyDROSLab/EF5-dockerized](https://github.com/HyDROSLab/EF5-dockerized) | Companion repository referenced for prebuilt CONUS CREST/SAC/KW parameter resources and broader EF5 workflow support assets. |

## Quality, validation, and known limitations

This project is practical and useful for iterative modeling work, but it should be treated as an evolving workflow rather than a fully hardened production system.

Please keep in mind:
- some paths, assumptions, and examples are environment-specific,
- edge cases across all basins/events may not be fully tested,
- upstream data service behavior/availability can change,
- there is room for improvement in robustness, error handling, and broader test coverage.

Before operational use, validate outputs for your basin/event and review intermediate products in the notebook.

## Suggested future improvements

- Add automated tests for date parsing, download logic, and CSV output schema.
- Add retry/backoff logic for transient network/API failures.
- Add structured logging and optional verbose/quiet modes.
- Add notebook checks to verify required files/directories before heavy processing steps.
