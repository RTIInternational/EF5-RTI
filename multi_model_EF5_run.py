from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from urllib.parse import urlencode
from urllib.request import urlopen

import argparse
import csv
import json
import subprocess

import geopandas as gpd
import pandas as pd
import rasterio
import dataretrieval.nwis as nwis
from pynhd import NLDI
from rasterio.mask import mask

# Plotly imports for visualization
try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    print("Warning: Plotly not available. Plots will be skipped.")
    PLOTLY_AVAILABLE = False

"""
Workflow Overview:

1. Read gage IDs from gages/gage_ids.csv
2. Delineate basin boundaries from USGS gages
3. Clip EF5 basic rasters for each basin
4. Download USGS observed streamflow
5. Build one EF5 control file per gage
6. Run the EF5 executable for each control file
7. Create interactive Plotly plots for successful runs

Expected project structure
- gages/
- data/EF5_US_Params/
- Forcings/
- BasicData/
- observations/
- Output/
- states/

Main command-line inputs
- --time-begin
- --time-end
- --model
- --freq
"""

def delineate_basin_from_gage(gage_id, out_dir):
    gage_id = str(gage_id).strip()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    basin_path = out_dir / f"{gage_id}_basin.geojson"
    site_path = out_dir / f"{gage_id}_gage.geojson"

    # If files already exist, load them and calculate needed values
    if basin_path.exists() and site_path.exists():
        basin_gdf = gpd.read_file(basin_path)
        site_gdf = gpd.read_file(site_path)

        if site_gdf.empty:
            raise ValueError(f"Gage file exists but is empty: {site_path}")
        if basin_gdf.empty:
            raise ValueError(f"Basin file exists but is empty: {basin_path}")

        lon = float(site_gdf.geometry.x.iloc[0])
        lat = float(site_gdf.geometry.y.iloc[0])

        basin_area_sqkm = basin_gdf.to_crs("EPSG:5070").geometry.area.sum() / 1_000_000.0

        output_paths = {
            "basin": basin_path,
            "gage": site_path,
        }

        return basin_gdf, site_gdf, lat, lon, basin_area_sqkm, output_paths

    # Pull site metadata from USGS
    site = nwis.get_info(sites=gage_id)[0]
    if site.empty:
        raise ValueError(f"No USGS site metadata found for gage {gage_id}")

    lat = float(site.iloc[0]["dec_lat_va"])
    lon = float(site.iloc[0]["dec_long_va"])

    site_gdf = gpd.GeoDataFrame(
        site.copy(),
        geometry=gpd.points_from_xy([lon], [lat]),
        crs="EPSG:4326",
    )

    # Delineate basin
    nldi = NLDI()
    basin_gdf = nldi.get_basins(
        [gage_id],
        fsource="nwissite",
        split_catchment=False,
        simplified=False,
    )

    if basin_gdf is None or basin_gdf.empty:
        raise ValueError(f"NLDI did not return a basin for gage {gage_id}")

    # Calculate basin area in square kilometers
    basin_area_sqkm = basin_gdf.to_crs("EPSG:5070").geometry.area.sum() / 1_000_000.0

    # Save outputs
    basin_gdf.to_file(basin_path, driver="GeoJSON")
    site_gdf.to_file(site_path, driver="GeoJSON")

    output_paths = {
        "basin": basin_path,
        "gage": site_path,
    }

    return basin_gdf, site_gdf, lat, lon, basin_area_sqkm, output_paths


def _process_one_gage(gage_id, out_dir):
    gage_id = str(gage_id).strip()

    try:
        _, _, lat, lon, basin_area_sqkm, output_paths = delineate_basin_from_gage(
            gage_id=gage_id,
            out_dir=out_dir,
        )

        return {
            "gage_id": gage_id,
            "status": "success",
            "latitude": lat,
            "longitude": lon,
            "basin_area_sqkm": basin_area_sqkm,
            "basin_path": str(output_paths["basin"]),
            "gage_path": str(output_paths["gage"]),
        }

    except Exception as e:
        return {
            "gage_id": gage_id,
            "status": "failed",
            "error": str(e),
        }


def delineate_basins_from_csv(max_workers=8):
    project_root = Path.cwd()
    gage_csv = project_root / "gages" / "gage_ids.csv"
    out_dir = project_root / "data" / "basin_delineations"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not gage_csv.exists():
        raise FileNotFoundError(f"Gage CSV not found: {gage_csv}")

    gages_df = pd.read_csv(gage_csv, dtype=str)

    if "gage_id" not in gages_df.columns:
        raise ValueError(
            f"Column 'gage_id' not found in {gage_csv}. "
            f"Available columns: {list(gages_df.columns)}"
        )

    gage_ids = [str(g).strip() for g in gages_df["gage_id"].dropna().tolist()]
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_gage = {
            executor.submit(_process_one_gage, gage_id, out_dir): gage_id
            for gage_id in gage_ids
        }

        for future in as_completed(future_to_gage):
            result = future.result()
            results.append(result)

            if result["status"] == "success":
                print(
                    f"Done: {result['gage_id']} | "
                    f"lat={result['latitude']:.6f}, "
                    f"lon={result['longitude']:.6f}, "
                    f"area={result['basin_area_sqkm']:.3f} km^2"
                )
            else:
                print(f"Failed: {result['gage_id']} -> {result['error']}")

    results_df = pd.DataFrame(results).sort_values("gage_id")
    results_df.to_csv(out_dir / "basin_delineation_summary.csv", index=False)

    return results_df

def clip_raster_to_basin(in_raster, basin_gdf, out_raster):
    in_raster = Path(in_raster)
    out_raster = Path(out_raster)
    out_raster.parent.mkdir(parents=True, exist_ok=True)

    if out_raster.exists():
        print(f"Skipping existing: {out_raster}")
        return str(out_raster)

    with rasterio.open(in_raster) as src:
        if basin_gdf.crs != src.crs:
            basin_proj = basin_gdf.to_crs(src.crs)
        else:
            basin_proj = basin_gdf

        shapes = [geom for geom in basin_proj.geometry if geom is not None and not geom.is_empty]
        if not shapes:
            raise ValueError(f"No valid geometry found for clipping {in_raster}")

        clipped_data, clipped_transform = mask(
            src,
            shapes,
            crop=True,
            nodata=src.nodata,
        )

        out_meta = src.meta.copy()
        out_meta.update(
            {
                "height": clipped_data.shape[1],
                "width": clipped_data.shape[2],
                "transform": clipped_transform,
            }
        )

        with rasterio.open(out_raster, "w", **out_meta) as dst:
            dst.write(clipped_data)

    return str(out_raster)

def get_max_fam_cell_coords(flow_accumulation_raster):
    """
    Find the highest-value cell in a clipped flow accumulation raster and
    return its value and center coordinates in EPSG:4326.

    Parameters
    ----------
    flow_accumulation_raster : str or Path
        Path to clipped flow accumulation raster.

    Returns
    -------
    dict
        Dictionary with:
        - fam_value
        - snapped_longitude
        - snapped_latitude
        - row
        - col
    """
    flow_accumulation_raster = Path(flow_accumulation_raster)

    with rasterio.open(flow_accumulation_raster) as src:
        data = src.read(1)
        nodata = src.nodata

        if nodata is not None:
            valid_mask = data != nodata
        else:
            # if nodata is not set, assume all finite cells are valid
            valid_mask = pd.notna(data)

        if not valid_mask.any():
            raise ValueError(f"No valid cells found in {flow_accumulation_raster}")

        masked = data.copy()
        masked[~valid_mask] = masked[valid_mask].min() - 1

        flat_index = masked.argmax()
        row, col = divmod(flat_index, masked.shape[1])

        fam_value = float(data[row, col])

        x, y = src.xy(row, col, offset="center")

        if src.crs and str(src.crs) != "EPSG:4326":
            point_gdf = gpd.GeoDataFrame(
                geometry=gpd.points_from_xy([x], [y]),
                crs=src.crs,
            ).to_crs("EPSG:4326")
            snapped_longitude = float(point_gdf.geometry.x.iloc[0])
            snapped_latitude = float(point_gdf.geometry.y.iloc[0])
        else:
            snapped_longitude = float(x)
            snapped_latitude = float(y)

    return {
        "fam_value": fam_value,
        "snapped_longitude": snapped_longitude,
        "snapped_latitude": snapped_latitude,
        "fam_row": int(row),
        "fam_col": int(col),
    }

def clip_main_layers_for_one_basin(basin_file, flow_direction_raster, flow_accumulation_raster, dem_raster, output_dir):
    basin_file = Path(basin_file)
    output_dir = Path(output_dir)

    gage_id = basin_file.stem.replace("_basin", "")
    basin_gdf = gpd.read_file(basin_file)

    if basin_gdf.empty:
        raise ValueError(f"Basin file is empty: {basin_file}")

    if basin_gdf.crs is None:
        raise ValueError(f"Basin file has no CRS: {basin_file}")

    flow_dir_out = output_dir / f"{gage_id}_flow_direction.tif"
    flow_acc_out = output_dir / f"{gage_id}_flow_accumulation.tif"
    dem_out = output_dir / f"{gage_id}_dem.tif"

    clip_raster_to_basin(flow_direction_raster, basin_gdf, flow_dir_out)
    clip_raster_to_basin(flow_accumulation_raster, basin_gdf, flow_acc_out)
    clip_raster_to_basin(dem_raster, basin_gdf, dem_out)

    fam_info = get_max_fam_cell_coords(flow_acc_out)

    return {
        "gage_id": gage_id,
        "status": "success",
        "basin_file": str(basin_file),
        "flow_direction": str(flow_dir_out),
        "flow_accumulation": str(flow_acc_out),
        "dem": str(dem_out),
        "fam_value": fam_info["fam_value"],
        "snapped_longitude": fam_info["snapped_longitude"],
        "snapped_latitude": fam_info["snapped_latitude"],
        "fam_row": fam_info["fam_row"],
        "fam_col": fam_info["fam_col"],
    }

def clip_main_layers_for_all_basins(max_workers=4):
    project_root = Path.cwd()

    basin_dir = project_root / "data" / "basin_delineations"
    output_dir = project_root / "BasicData"
    output_dir.mkdir(parents=True, exist_ok=True)

    flow_direction_raster = project_root / "data" / "EF5_US_Params" / "basic" / "fdir_usa.tif"
    flow_accumulation_raster = project_root / "data" / "EF5_US_Params" / "basic" / "facc_usa.tif"
    dem_raster = project_root / "data" / "EF5_US_Params" / "basic" / "dem_usa.tif"

    basin_files = sorted(basin_dir.glob("*_basin.geojson"))

    if not basin_files:
        raise FileNotFoundError(f"No basin files found in {basin_dir}")

    results = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                clip_main_layers_for_one_basin,
                basin_file,
                flow_direction_raster,
                flow_accumulation_raster,
                dem_raster,
                output_dir,
            ): basin_file
            for basin_file in basin_files
        }

        for future in as_completed(futures):
            basin_file = futures[future]
            gage_id = basin_file.stem.replace("_basin", "")

            try:
                result = future.result()
                results.append(result)
                print(f"Done: {gage_id}")

            except Exception as e:
                results.append({
                    "gage_id": gage_id,
                    "status": "failed",
                    "basin_file": str(basin_file),
                    "error": str(e),
                })
                print(f"Failed: {gage_id} -> {e}")

    results_df = pd.DataFrame(results).sort_values("gage_id")
    results_df.to_csv(output_dir / "main_layer_clipping_summary.csv", index=False)

    # Update basin delineation summary with snapped outlet coordinates
    basin_summary_csv = project_root / "data" / "basin_delineations" / "basin_delineation_summary.csv"

    if basin_summary_csv.exists():
        basin_summary_df = pd.read_csv(basin_summary_csv, dtype={"gage_id": str})

        clip_cols = [
            "gage_id",
            "fam_value",
            "snapped_longitude",
            "snapped_latitude",
            "fam_row",
            "fam_col",
        ]

        # remove old versions of these columns if they already exist
        for col in clip_cols[1:]:
            if col in basin_summary_df.columns:
                basin_summary_df = basin_summary_df.drop(columns=col)

        merged_df = basin_summary_df.merge(
            results_df[clip_cols],
            on="gage_id",
            how="left",
        )

        merged_df.to_csv(basin_summary_csv, index=False)

    return results_df

CFS_TO_CMS = 0.028316846592


def parse_ef5_time(value: str) -> datetime:
    """
    Parse EF5-style datetime string: YYYYMMDDHHMMSS
    """
    return datetime.strptime(str(value).strip(), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


def fetch_usgs_iv(gage_id: str, start_utc: datetime, end_utc: datetime):
    """
    Fetch USGS instantaneous discharge (parameter 00060) and convert from cfs to cms.
    """
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

        q_cms = q_cfs * CFS_TO_CMS
        points.append((dt, q_cms))

    points.sort(key=lambda x: x[0])
    return points


def write_usgs_csv(rows, out_csv: Path):
    """
    Write USGS streamflow rows to CSV.
    Overwrites existing file every run.
    """
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Date (UTC)", "Q (cms)"])
        for dt, q in rows:
            writer.writerow([dt.strftime("%Y-%m-%d %H:%M:%S"), f"{q:.6f}"])


def fetch_usgs_for_one_gage(gage_id: str, time_begin: str, time_end: str, out_dir: Path):
    """
    Download USGS IV streamflow for one gage and write EF5-ready observation CSV.
    """
    gage_id = str(gage_id).strip()
    out_dir = Path(out_dir)

    start_utc = parse_ef5_time(time_begin)
    end_utc = parse_ef5_time(time_end)

    if end_utc <= start_utc:
        raise ValueError("time_end must be after time_begin")

    raw_points = fetch_usgs_iv(gage_id, start_utc, end_utc)
    if not raw_points:
        raise RuntimeError(f"No USGS streamflow data returned for gage {gage_id}")

    rows = [(dt, q) for dt, q in raw_points if start_utc <= dt <= end_utc]

    out_csv = out_dir / f"Streamflow_Time_Series_CMS_UTC_USGS_{gage_id}.csv"
    write_usgs_csv(rows, out_csv)

    return {
        "gage_id": gage_id,
        "status": "success",
        "rows_written": len(rows),
        "output_csv": str(out_csv),
    }


def fetch_usgs_for_all_gages(time_begin: str, time_end: str, max_workers=8):
    """
    Read gage IDs from gages/gage_ids.csv and download USGS IV streamflow
    for all gages in parallel for the specified EF5-style UTC datetime range.
    """
    project_root = Path.cwd()
    gage_csv = project_root / "gages" / "gage_ids.csv"
    out_dir = project_root / "observations"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not gage_csv.exists():
        raise FileNotFoundError(f"Gage CSV not found: {gage_csv}")

    gages_df = pd.read_csv(gage_csv, dtype=str)

    if "gage_id" not in gages_df.columns:
        raise ValueError(
            f"Column 'gage_id' not found in {gage_csv}. "
            f"Available columns: {list(gages_df.columns)}"
        )

    gage_ids = [str(g).strip() for g in gages_df["gage_id"].dropna().tolist()]
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_gage = {
            executor.submit(
                fetch_usgs_for_one_gage,
                gage_id,
                time_begin,
                time_end,
                out_dir,
            ): gage_id
            for gage_id in gage_ids
        }

        for future in as_completed(future_to_gage):
            gage_id = future_to_gage[future]

            try:
                result = future.result()
                results.append(result)
                print(f"Done: {gage_id}")

            except Exception as e:
                results.append({
                    "gage_id": gage_id,
                    "status": "failed",
                    "error": str(e),
                })
                print(f"Failed: {gage_id} -> {e}")

    results_df = pd.DataFrame(results).sort_values("gage_id")
    results_df.to_csv(out_dir / "usgs_download_summary.csv", index=False)

    return results_df

def ef5_datetime_to_control_time(value: str) -> str:
    value = str(value).strip()

    if len(value) == 14:
        dt = datetime.strptime(value, "%Y%m%d%H%M%S")
        return dt.strftime("%Y%m%d%H%M")

    if len(value) == 12:
        dt = datetime.strptime(value, "%Y%m%d%H%M")
        return dt.strftime("%Y%m%d%H%M")

    raise ValueError(
        f"Invalid datetime '{value}'. Use YYYYMMDDHHMMSS or YYYYMMDDHHMM."
    )


def normalize_model_name(model_to_run: str) -> str:
    model = str(model_to_run).strip().upper()

    if model not in {"CREST", "SAC", "HP"}:
        raise ValueError("model_to_run must be one of: CREST, SAC, HP")

    return model


def build_precip_block(freq: str) -> tuple[str, str]:
    """
    Return:
    - precip block text
    - precip forcing name to use in task blocks
    """
    freq = str(freq).strip()

    if freq == "2u":
        precip_name = "MRMS_GRIB"
        precip_block = f"""[PrecipForcing {precip_name}]
TYPE=GRIB2
UNIT=mm/h
FREQ=2u
LOC=Forcings/Precipitation/2min
NAME=PrecipRate_00.00_YYYYMMDD-HHUU00.grib2
"""
    else:
        precip_name = "MRMS"
        precip_block = f"""[PrecipForcing {precip_name}]
TYPE=GRIB2
UNIT=mm/h
FREQ={freq}
LOC=Forcings/Precipitation/hourly
NAME=RadarOnly_QPE_01H_00.00_YYYYMMDD-HH0000.grib2.gz
"""

    return precip_block, precip_name


def build_control_file_text(
    gage_id: str,
    latitude: float,
    longitude: float,
    basin_area_sqkm: float,
    time_begin: str,
    time_end: str,
    freq: str,
    model_to_run: str,
) -> str:
    gage_id = str(gage_id).strip()
    model_to_run = normalize_model_name(model_to_run)

    time_begin_ctrl = ef5_datetime_to_control_time(time_begin)
    time_end_ctrl = ef5_datetime_to_control_time(time_end)

    execute_task_lookup = {
        "CREST": f"Run{gage_id}crest",
        "SAC": f"Run{gage_id}sac",
        "HP": f"Run{gage_id}hp",
    }
    execute_task = execute_task_lookup[model_to_run]

    basin_area_for_control = int(round(float(basin_area_sqkm)))

    precip_block, precip_name = build_precip_block(freq)

    crest_output_folder = f"Output/{gage_id}/crest/"
    sac_output_folder = f"Output/{gage_id}/sac/"
    hp_output_folder = f"Output/{gage_id}/hp/"

    crest_states_folder = f"states/{gage_id}/crest/"
    sac_states_folder = f"states/{gage_id}/sac/"
    hp_states_folder = f"states/{gage_id}/hp/"

    control_text = f"""[Basic]
DEM=BasicData/{gage_id}_dem.tif
DDM=BasicData/{gage_id}_flow_direction.tif
FAM=BasicData/{gage_id}_flow_accumulation.tif
PROJ=geographic
ESRIDDM=true
SelfFAM=false

{precip_block}
[PETForcing PET]
TYPE=TIF
UNIT=mm/d
FREQ=1m
LOC=Forcings/PET/
NAME=PET_MM_usa.tif

[Gauge {gage_id}]
LON={longitude}
LAT={latitude}
OBS=observations/Streamflow_Time_Series_CMS_UTC_USGS_{gage_id}.csv
OUTPUTTS=true
BASINAREA={basin_area_for_control}
WANTCO=true

[Basin {gage_id}]
GAUGE={gage_id}

[CrestParamSet {gage_id}crest]
wm_grid=data/EF5_US_Params/crest_params/wm_usa.tif
b_grid=data/EF5_US_Params/crest_params/b_usa.tif
im_grid=data/EF5_US_Params/crest_params/im_usa.tif
fc_grid=data/EF5_US_Params/crest_params/ksat_usa.tif
gauge={gage_id}
wm=1.0
b=1.0
im=1.0 
ke=1.0
fc=1.0 
iwu=24.999905

[SacParamSet {gage_id}sac]
UZTWM_grid=data/EF5_US_Params/sac_params/uztwm_usa.tif
UZFWM_grid=data/EF5_US_Params/sac_params/uzfwm_usa.tif
UZK_grid=data/EF5_US_Params/sac_params/uzk_usa.tif
ZPERC_grid=data/EF5_US_Params/sac_params/zperc_usa.tif
REXP_grid=data/EF5_US_Params/sac_params/rexp_usa.tif
LZTWM_grid=data/EF5_US_Params/sac_params/lztwm_usa.tif
LZFSM_grid=data/EF5_US_Params/sac_params/lzfsm_usa.tif
LZFPM_grid=data/EF5_US_Params/sac_params/lzfpm_usa.tif
LZSK_grid=data/EF5_US_Params/sac_params/lzsk_usa.tif
LZPK_grid=data/EF5_US_Params/sac_params/lzpk_usa.tif
PFREE_grid=data/EF5_US_Params/sac_params/pfree_usa.tif
gauge={gage_id}
UZTWM=1.0
UZFWM=1.0
UZK=1.0
PCTIM=0.101
ADIMP=0.10
RIVA=1.001
ZPERC=1.0
REXP=1.0
LZTWM=1.0
LZFSM=1.0
LZFPM=1.0
LZSK=1.0
LZPK=1.0
PFREE=1.0
SIDE=0.0
RSERV=0.3
ADIMC=1.0
UZTWC=0.55
UZFWC=0.14
LZTWC=0.56
LZFSC=0.11
LZFPC=0.46

[HPParamSet {gage_id}hp]
gauge={gage_id}
precip=1.0
split=1.0 

[kwparamset {gage_id}kw]
under_grid=data/EF5_US_Params/kw_params/ksat_usa.tif
leaki_grid=data/EF5_US_Params/kw_params/leaki_usa.tif
alpha_grid=data/EF5_US_Params/kw_params/alpha_usa.tif
beta_grid=data/EF5_US_Params/kw_params/beta_usa.tif
alpha0_grid=data/EF5_US_Params/kw_params/alpha0_usa.tif
gauge={gage_id}
alpha0=1.0
alpha=1.0
beta=1.0
under=1
leaki=1.0
th=10.0
isu=00.0

[Task Run{gage_id}crest]
STYLE=SIMU
MODEL=CREST
ROUTING=KW
ROUTING_PARAM_SET={gage_id}kw
BASIN={gage_id}
PRECIP={precip_name}
PET=PET
OUTPUT={crest_output_folder}
STATES={crest_states_folder}
output_grids=MAXUNITSTREAMFLOW|MAXSTREAMFLOW
TIMESTEP={freq}
TIME_BEGIN={time_begin_ctrl}
TIME_END={time_end_ctrl}
PARAM_SET={gage_id}crest

[Task Run{gage_id}sac]
STYLE=SIMU
MODEL=SAC
ROUTING=KW
ROUTING_PARAM_SET={gage_id}kw
BASIN={gage_id}
PRECIP={precip_name}
PET=PET
OUTPUT={sac_output_folder}
STATES={sac_states_folder}
output_grids=MAXUNITSTREAMFLOW|MAXSTREAMFLOW
TIMESTEP={freq}
TIME_BEGIN={time_begin_ctrl}
TIME_END={time_end_ctrl}
PARAM_SET={gage_id}sac

[Task Run{gage_id}hp]
STYLE=SIMU
MODEL=HP
ROUTING=KW
ROUTING_PARAM_SET={gage_id}kw
BASIN={gage_id}
PRECIP={precip_name}
PET=PET
OUTPUT={hp_output_folder}
STATES={hp_states_folder}
output_grids=MAXUNITSTREAMFLOW|MAXSTREAMFLOW
TIMESTEP={freq}
TIME_BEGIN={time_begin_ctrl}
TIME_END={time_end_ctrl}
PARAM_SET={gage_id}hp

[Execute]
TASK={execute_task}
"""
    return control_text


def _create_one_control_file(
    gage_id: str,
    summary_lookup: dict,
    out_dir: Path,
    project_root: Path,
    time_begin: str,
    time_end: str,
    model_to_run: str,
    freq: str,
):
    gage_id = str(gage_id).strip()

    if gage_id not in summary_lookup:
        return {
            "gage_id": gage_id,
            "status": "failed",
            "error": "gage_id not found in basin_delineation_summary.csv",
        }

    row = summary_lookup[gage_id]

    try:
        crest_output_dir = project_root / "Output" / gage_id / "crest"
        sac_output_dir = project_root / "Output" / gage_id / "sac"
        hp_output_dir = project_root / "Output" / gage_id / "hp"

        crest_states_dir = project_root / "states" / gage_id / "crest"
        sac_states_dir = project_root / "states" / gage_id / "sac"
        hp_states_dir = project_root / "states" / gage_id / "hp"

        crest_output_dir.mkdir(parents=True, exist_ok=True)
        sac_output_dir.mkdir(parents=True, exist_ok=True)
        hp_output_dir.mkdir(parents=True, exist_ok=True)

        crest_states_dir.mkdir(parents=True, exist_ok=True)
        sac_states_dir.mkdir(parents=True, exist_ok=True)
        hp_states_dir.mkdir(parents=True, exist_ok=True)

        control_text = build_control_file_text(
            gage_id=gage_id,
            latitude=float(row["snapped_latitude"]),
            longitude=float(row["snapped_longitude"]),
            basin_area_sqkm=float(row["basin_area_sqkm"]),
            time_begin=time_begin,
            time_end=time_end,
            freq=freq,
            model_to_run=model_to_run,
        )

        out_file = out_dir / f"control_{gage_id}.txt"
        out_file.write_text(control_text, encoding="utf-8")

        return {
            "gage_id": gage_id,
            "status": "success",
            "control_file": str(out_file),
            "crest_output_folder": str(crest_output_dir),
            "sac_output_folder": str(sac_output_dir),
            "hp_output_folder": str(hp_output_dir),
            "crest_states_folder": str(crest_states_dir),
            "sac_states_folder": str(sac_states_dir),
            "hp_states_folder": str(hp_states_dir),
            "model_to_run": normalize_model_name(model_to_run),
            "freq": freq,
        }

    except Exception as e:
        return {
            "gage_id": gage_id,
            "status": "failed",
            "error": str(e),
        }


def create_control_files_for_all_gages(
    time_begin: str,
    time_end: str,
    model_to_run: str,
    freq: str = "1h",
    max_workers: int = 8,
):
    project_root = Path.cwd()

    gage_csv = project_root / "gages" / "gage_ids.csv"
    summary_csv = project_root / "data" / "basin_delineations" / "basin_delineation_summary.csv"
    out_dir = project_root

    if not gage_csv.exists():
        raise FileNotFoundError(f"Gage CSV not found: {gage_csv}")

    if not summary_csv.exists():
        raise FileNotFoundError(f"Basin summary CSV not found: {summary_csv}")

    gages_df = pd.read_csv(gage_csv, dtype={"gage_id": str})
    summary_df = pd.read_csv(summary_csv, dtype={"gage_id": str})

    if "gage_id" not in gages_df.columns:
        raise ValueError(
            f"Column 'gage_id' not found in {gage_csv}. "
            f"Available columns: {list(gages_df.columns)}"
        )

    required_summary_cols = {
        "gage_id",
        "basin_area_sqkm",
        "snapped_latitude",
        "snapped_longitude",
    }

    missing_cols = required_summary_cols - set(summary_df.columns)
    if missing_cols:
        raise ValueError(
            f"Missing required columns in {summary_csv}: {sorted(missing_cols)}"
        )

    summary_lookup = summary_df.set_index("gage_id").to_dict(orient="index")
    gage_ids = [str(g).strip() for g in gages_df["gage_id"].dropna().tolist()]
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_gage = {
            executor.submit(
                _create_one_control_file,
                gage_id,
                summary_lookup,
                out_dir,
                project_root,
                time_begin,
                time_end,
                model_to_run,
                freq,
            ): gage_id
            for gage_id in gage_ids
        }

        for future in as_completed(future_to_gage):
            gage_id = future_to_gage[future]
            result = future.result()
            results.append(result)

            if result["status"] == "success":
                print(f"Created: {result['control_file']}")
            else:
                print(f"Failed: {gage_id} -> {result['error']}")

    results_df = pd.DataFrame(results).sort_values("gage_id")
    results_df.to_csv(project_root / "control_file_creation_summary.csv", index=False)

    return results_df
def run_ef5_for_one_control(control_file: Path):
    control_file = Path(control_file)
    project_root = control_file.parent
    ef5_exe = project_root / "ef5"

    if not control_file.exists():
        return {
            "control_file": str(control_file),
            "status": "failed",
            "error": "control file not found",
        }

    if not ef5_exe.exists():
        return {
            "control_file": str(control_file),
            "status": "failed",
            "error": f"EF5 executable not found: {ef5_exe}",
        }

    gage_id = control_file.stem.replace("control_", "")
    log_file = project_root / f"ef5_run_{gage_id}.log"

    try:
        with log_file.open("w", encoding="utf-8") as log:
            result = subprocess.run(
                ["./ef5", control_file.name],
                cwd=str(project_root),
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )

        if result.returncode == 0:
            return {
                "gage_id": gage_id,
                "control_file": str(control_file),
                "log_file": str(log_file),
                "status": "success",
                "return_code": result.returncode,
            }
        else:
            return {
                "gage_id": gage_id,
                "control_file": str(control_file),
                "log_file": str(log_file),
                "status": "failed",
                "return_code": result.returncode,
            }

    except Exception as e:
        return {
            "gage_id": gage_id,
            "control_file": str(control_file),
            "log_file": str(log_file),
            "status": "failed",
            "error": str(e),
        }


def run_ef5_for_all_controls(max_workers: int = 4):
    project_root = Path.cwd()
    control_files = sorted(project_root.glob("control_*.txt"))

    if not control_files:
        raise FileNotFoundError(f"No control files found in {project_root}")

    ef5_exe = project_root / "ef5"
    if not ef5_exe.exists():
        raise FileNotFoundError(f"EF5 executable not found: {ef5_exe}")

    results = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_control = {
            executor.submit(run_ef5_for_one_control, control_file): control_file
            for control_file in control_files
        }

        for future in as_completed(future_to_control):
            control_file = future_to_control[future]

            try:
                result = future.result()
                results.append(result)

                if result["status"] == "success":
                    print(f"Done: {control_file.name}")
                else:
                    print(f"Failed: {control_file.name}")

            except Exception as e:
                results.append({
                    "control_file": str(control_file),
                    "status": "failed",
                    "error": str(e),
                })
                print(f"Failed: {control_file.name} -> {e}")

    results_df = pd.DataFrame(results).sort_values("control_file")
    results_df.to_csv(project_root / "ef5_execution_summary.csv", index=False)

    return results_df


def create_plotly_plot_for_gage(
    gage_id: str,
    model_name: str,
    csv_file: Path,
    output_dir: Path,
):
    """
    Create interactive Plotly plot for one gage/model combination.
    
    Args:
        gage_id: USGS gage ID
        model_name: Model name (CREST, SAC, HP)
        csv_file: Path to EF5 output CSV file
        output_dir: Directory where to save HTML plot
    
    Returns:
        dict with status and file path information
    """
    if not PLOTLY_AVAILABLE:
        return {
            "gage_id": gage_id,
            "model": model_name,
            "status": "skipped",
            "error": "Plotly not available",
        }
    
    try:
        if not csv_file.exists():
            return {
                "gage_id": gage_id,
                "model": model_name,
                "status": "failed",
                "error": f"CSV file not found: {csv_file}",
            }
        
        # Read and prepare data
        df = pd.read_csv(csv_file)
        df.columns = [c.strip() for c in df.columns]
        df["Time"] = pd.to_datetime(df["Time"])
        
        df["Discharge(m^3 s^-1)"] = pd.to_numeric(df["Discharge(m^3 s^-1)"], errors="coerce")
        df["Observed(m^3 s^-1)"] = pd.to_numeric(df["Observed(m^3 s^-1)"], errors="coerce")
        df["Precip(mm h^-1)"] = pd.to_numeric(df["Precip(mm h^-1)"], errors="coerce")
        
        # Interpolate observed values for plotting
        df["Observed_interp"] = df["Observed(m^3 s^-1)"].interpolate(method="linear")
        
        # Create subplot with secondary y-axis
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add simulated discharge trace (dashed blue)
        fig.add_trace(
            go.Scatter(
                x=df["Time"],
                y=df["Discharge(m^3 s^-1)"],
                mode="lines",
                name="Simulated Discharge",
                line=dict(width=2, color="blue", dash="dash")
            ),
            secondary_y=False,
        )
        
        # Add observed streamflow trace (solid orange)
        fig.add_trace(
            go.Scatter(
                x=df["Time"],
                y=df["Observed_interp"],
                mode="lines",
                name="Observed Streamflow",
                line=dict(width=2, color="orange")
            ),
            secondary_y=False,
        )
        
        # Add precipitation trace (on secondary y-axis)
        fig.add_trace(
            go.Bar(
                x=df["Time"],
                y=df["Precip(mm h^-1)"],
                name="Precipitation",
                opacity=0.35,
                marker_color="lightblue"
            ),
            secondary_y=True,
        )
        
        # Set x-axis title
        fig.update_xaxes(title_text="Time")
        
        # Set y-axes titles
        fig.update_yaxes(title_text="Streamflow (m³/s)", secondary_y=False)
        fig.update_yaxes(title_text="Precipitation (mm/h)", secondary_y=True)
        
        # Invert precipitation axis
        fig.update_yaxes(autorange="reversed", secondary_y=True)
        
        # Update layout
        fig.update_layout(
            title=f"{model_name.upper()} Model - Gage {gage_id}: Observed vs Simulated Streamflow with Precipitation",
            xaxis=dict(rangeslider_visible=True),
            template="plotly_white",
            hovermode="x unified",
            legend=dict(x=0.01, y=0.99),
            height=600,
            width=1200
        )
        
        # Save as HTML file
        html_file = output_dir / f"{model_name.upper()}_{gage_id}_plot.html"
        fig.write_html(html_file)
        
        return {
            "gage_id": gage_id,
            "model": model_name,
            "status": "success",
            "html_file": str(html_file),
            "csv_file": str(csv_file),
        }
    
    except Exception as e:
        return {
            "gage_id": gage_id,
            "model": model_name,
            "status": "failed",
            "error": str(e),
        }


def create_plots_for_all_successful_runs(
    ef5_results_df: pd.DataFrame,
    control_results_df: pd.DataFrame,
    max_workers: int = 4,
):
    """
    Create Plotly plots for all successful EF5 runs.
    
    Args:
        ef5_results_df: Results from EF5 execution
        control_results_df: Results from control file creation
        max_workers: Number of parallel workers for plot creation
    
    Returns:
        DataFrame with plot creation results
    """
    if not PLOTLY_AVAILABLE:
        print("Plotly not available. Skipping plot creation.")
        return pd.DataFrame()
    
    project_root = Path.cwd()
    
    # Get successful EF5 runs
    successful_runs = ef5_results_df[ef5_results_df["status"] == "success"]
    
    if successful_runs.empty:
        print("No successful EF5 runs found. Skipping plot creation.")
        return pd.DataFrame()
    
    # Create lookup for control file info
    control_lookup = {}
    for _, row in control_results_df.iterrows():
        if row["status"] == "success":
            control_lookup[row["gage_id"]] = row
    
    plot_tasks = []
    
    # Create plot tasks for each successful run
    for _, ef5_row in successful_runs.iterrows():
        gage_id = ef5_row["gage_id"]
        
        if gage_id not in control_lookup:
            continue
        
        control_row = control_lookup[gage_id]
        model_name = control_row["model_to_run"].lower()
        
        # Determine CSV file path and output directory
        output_dir = project_root / "Output" / gage_id / model_name
        csv_file = output_dir / f"ts.{gage_id}.{model_name}.csv"
        
        plot_tasks.append({
            "gage_id": gage_id,
            "model_name": model_name,
            "csv_file": csv_file,
            "output_dir": output_dir,
        })
    
    if not plot_tasks:
        print("No plotting tasks generated. Skipping plot creation.")
        return pd.DataFrame()
    
    print(f"Creating plots for {len(plot_tasks)} gage/model combinations...")
    
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(
                create_plotly_plot_for_gage,
                task["gage_id"],
                task["model_name"],
                task["csv_file"],
                task["output_dir"],
            ): task
            for task in plot_tasks
        }
        
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            
            try:
                result = future.result()
                results.append(result)
                
                if result["status"] == "success":
                    print(f"Plot created: {result['gage_id']} {result['model'].upper()}")
                else:
                    print(f"Plot failed: {result['gage_id']} {result['model'].upper()} -> {result.get('error', 'Unknown error')}")
            
            except Exception as e:
                results.append({
                    "gage_id": task["gage_id"],
                    "model": task["model_name"],
                    "status": "failed",
                    "error": str(e),
                })
                print(f"Plot failed: {task['gage_id']} {task['model_name'].upper()} -> {e}")
    
    results_df = pd.DataFrame(results).sort_values(["gage_id", "model"])
    results_df.to_csv(project_root / "plot_creation_summary.csv", index=False)
    
    return results_df

def run_full_ef5_setup(
    time_begin: str,
    time_end: str,
    model_to_run: str,
    freq: str = "1h",
    basin_workers: int = 8,
    clip_workers: int = 4,
    usgs_workers: int = 8,
    control_workers: int = 8,
    ef5_workers: int = 4,
    plot_workers: int = 4,
    create_plots: bool = True,
):
    print("\n--- Step 1: Delineating basins ---")
    basin_results = delineate_basins_from_csv(max_workers=basin_workers)

    print("\n--- Step 2: Clipping main raster layers ---")
    clip_results = clip_main_layers_for_all_basins(max_workers=clip_workers)

    print("\n--- Step 3: Downloading USGS observations ---")
    usgs_results = fetch_usgs_for_all_gages(
        time_begin=time_begin,
        time_end=time_end,
        max_workers=usgs_workers,
    )

    print("\n--- Step 4: Creating control files ---")
    control_results = create_control_files_for_all_gages(
        time_begin=time_begin,
        time_end=time_end,
        model_to_run=model_to_run,
        freq=freq,
        max_workers=control_workers,
    )

    print("\n--- Step 5: Running EF5 executable ---")
    ef5_results = run_ef5_for_all_controls(max_workers=ef5_workers)

    # Step 6: Create interactive plots (if requested and successful runs exist)
    plot_results = pd.DataFrame()
    if create_plots and PLOTLY_AVAILABLE:
        print("\n--- Step 6: Creating interactive Plotly plots ---")
        plot_results = create_plots_for_all_successful_runs(
            ef5_results_df=ef5_results,
            control_results_df=control_results,
            max_workers=plot_workers,
        )
    elif create_plots and not PLOTLY_AVAILABLE:
        print("\n--- Step 6: Skipping plots (Plotly not available) ---")
    else:
        print("\n--- Step 6: Skipping plots (disabled by user) ---")

    return {
        "basins": basin_results,
        "clipping": clip_results,
        "usgs": usgs_results,
        "control_files": control_results,
        "ef5_runs": ef5_results,
        "plots": plot_results,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Run full EF5 setup workflow for all gages in gages/gage_ids.csv"
    )

    parser.add_argument(
        "--time-begin",
        required=True,
        help="Start time in UTC, format YYYYMMDDHHMMSS",
    )
    parser.add_argument(
        "--time-end",
        required=True,
        help="End time in UTC, format YYYYMMDDHHMMSS",
    )
    parser.add_argument(
        "--model",
        required=True,
        choices=["HP", "SAC", "CREST", "hp", "sac", "crest"],
        help="Model to run in the [Execute] block",
    )
    parser.add_argument(
        "--freq",
        default="1h",
        help="Model and forcing frequency, for example 1h or 2u",
    )
    parser.add_argument(
        "--basin-workers",
        type=int,
        default=8,
        help="Number of workers for basin delineation",
    )
    parser.add_argument(
        "--clip-workers",
        type=int,
        default=4,
        help="Number of workers for raster clipping",
    )
    parser.add_argument(
        "--usgs-workers",
        type=int,
        default=8,
        help="Number of workers for USGS downloads",
    )
    parser.add_argument(
        "--control-workers",
        type=int,
        default=8,
        help="Number of workers for control-file creation",
    )
    parser.add_argument(
        "--ef5-workers",
        type=int,
        default=4,
        help="Number of parallel EF5 runs",
    )
    parser.add_argument(
        "--plot-workers",
        type=int,
        default=4,
        help="Number of parallel workers for plot creation",
    )
    parser.add_argument(
        "--no-plots",
        action="store_true",
        help="Skip creating interactive Plotly plots",
    )

    args = parser.parse_args()

    results = run_full_ef5_setup(
        time_begin=args.time_begin,
        time_end=args.time_end,
        model_to_run=args.model.upper(),
        freq=args.freq,
        basin_workers=args.basin_workers,
        clip_workers=args.clip_workers,
        usgs_workers=args.usgs_workers,
        control_workers=args.control_workers,
        ef5_workers=args.ef5_workers,
        plot_workers=args.plot_workers,
        create_plots=not args.no_plots,
    )

    print("\nWorkflow complete.")
    print("\nBasin summary:")
    print(results["basins"])

    print("\nClipping summary:")
    print(results["clipping"])

    print("\nUSGS summary:")
    print(results["usgs"])

    print("\nControl file summary:")
    print(results["control_files"])

    print("\nEF5 execution summary:")
    print(results["ef5_runs"])

    if not results["plots"].empty:
        print("\nPlot creation summary:")
        print(results["plots"])
    else:
        print("\nNo plots were created.")

    if not results["plots"].empty:
        print("\nPlot creation summary:")
        print(results["plots"])
    else:
        print("\nNo plots were created.")


if __name__ == "__main__":
    main()