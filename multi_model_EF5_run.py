from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from urllib.parse import urlencode
from urllib.request import urlopen
from functools import lru_cache

import argparse
import csv
import json
import subprocess
import re

import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.mask import mask

"""
EF5 Multi-Model Hydrological Simulation Framework

This script orchestrates a complete end-to-end hydrological modeling workflow using the EF5 
(Ensemble Framework for Flash Flood Forecasting) model. It processes multiple stream gages 
in parallel, supporting three hydrological models: CREST, SAC-SMA, and HP.

=== WORKFLOW OVERVIEW ===

    ┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
    │  1. Basin       │────▶│  2. Raster       │────▶│  3. USGS Data   │
    │  Delineation    │     │  Clipping        │     │  Download       │
    └─────────────────┘     └──────────────────┘     └─────────────────┘
           │                         │                         │
           ▼                         ▼                         ▼
    Read gages/gage_ids.csv    Clip DEM, flow dir,      Download observed
    → Load pre-computed        flow accumulation        streamflow from
    → basin parquet files      → Find max FAM coords    USGS Instantaneous
    → Save GeoJSON files       → Save to BasicData/     Values (IV) API
    
    ┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
    │  4. Control     │────▶│  5. EF5          │────▶│  6. Visualization │
    │  File Creation  │     │  Execution       │     │  (Optional)      │
    └─────────────────┘     └──────────────────┘     └─────────────────┘
           │                         │                         │
           ▼                         ▼                         ▼
    Merge basin+raster+obs    Run ./ef5 executable      Create interactive
    → Build EF5 config        subprocess for each       Plotly HTML plots
    → Generate control_*.txt  control file              comparing observed
    files with model params  → Output/ and states/     vs. modeled flows

=== EXPECTED PROJECT STRUCTURE ===

    project_root/
    ├── gages/gage_ids.csv              # Input: List of USGS gage IDs (must include 'state' column)
    ├── data/EF5_US_Params/             # Input: CONUS model parameter grids
    │   ├── basic/{dem,fdir,facc}_usa.tif
    │   ├── crest_params/*.tif
    │   ├── sac_params/*.tif
    │   └── kw_params/*.tif
    ├── data/EF5-oCONUS-Parameters/     # Input: Regional (AK/HI/PR) parameter grids
    │   ├── basic/{ak,hi,carib}_{dem,fdir,facc}_*.tif
    │   ├── parameters/{CREST,KW}/*.tif
    │   └── pet/FAO.PET.MM.tif
    ├── Forcings/                       # Input: Precipitation and PET data
    │   ├── Precipitation/
    │   │   ├── 2min/{CONUS,AK,HI,PR}/ # 2-minute MRMS data (regional)
    │   │   └── hourly/{CONUS,AK,HI,PR}/ # Hourly QPE data (regional)
    │   └── PET/
    ├── ef5                            # Input: EF5 executable
    ├── data/basin_delineations/       # Created: Basin boundary files
    ├── BasicData/                     # Created: Clipped rasters (region-specific)
    ├── observations/                  # Created: USGS streamflow CSVs
    ├── Control_Files/control_*.txt    # Created: EF5 configuration files
    ├── Output/$gage_id/{crest,sac,hp}/ # Created: Model simulation results
    └── states/$gage_id/{crest,sac,hp}/ # Created: Model state files

=== COMMAND-LINE INTERFACE ===

python multi_model_EF5_run.py
    --time-begin YYYYMMDDHHMMSS         # Simulation start time
    --time-end YYYYMMDDHHMMSS           # Simulation end time
    --model {CREST,SAC,HP}              # Hydrological model selection
    --freq {1h,2u}                     # Time step (1h=hourly, 2u=2-minute)

=== TECHNICAL DETAILS ===

- Parallel Processing: Uses ThreadPoolExecutor for I/O-bound operations (API calls, file I/O) 
  and ProcessPoolExecutor for CPU-bound operations (raster processing, model execution)
- Coordinate Systems: Handles transformations between EPSG:4326 (WGS84) and EPSG:5070 
  (Albers Equal Area Conic CONUS)
- Unit Conversions: USGS streamflow from cubic feet per second (CFS) to cubic meters 
  per second (CMS) using conversion factor 0.028316846592
- Error Handling: Comprehensive try/catch with detailed error reporting and summary CSVs
- Data Validation: Extensive checks for file existence, coordinate system validity, 
  and data completeness at each workflow stage

=== DOMAIN-SPECIFIC TERMINOLOGY ===

- EF5: Ensemble Framework for Flash Flood Forecasting - a distributed hydrological model
- CREST: Coupled Routing and Excess Storage model - rainfall-runoff model
- SAC-SMA: Sacramento Soil Moisture Accounting model - conceptual rainfall-runoff model  
- HP: Hydrologic Prediction model - simplified linear reservoir model
- KW: Kinematic Wave routing - method for routing streamflow through channels
- FAM/Flow Accumulation: Raster showing accumulated upstream drainage area for each cell
- Basin Outlet: Point of maximum flow accumulation, used as the computational streamflow location
"""

# Plotly imports for visualization
try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    print("Warning: Plotly not available. Plots will be skipped.")
    PLOTLY_AVAILABLE = False

def normalize_gage_id(value):
    """
    Normalize gage IDs by removing 'usgs-' prefix to match gage_ids.csv format.
    
    Parameters
    ----------
    value : str or int
        Gage ID with or without 'usgs-' prefix
        
    Returns
    -------
    str
        Normalized gage ID (numeric portion only)
    """
    if pd.isna(value):
        return pd.NA
    s = str(value).strip()
    s = re.sub(r"(?i)^usgs-", "", s)
    return s


REGION_BY_STATE = {
    "AK": "ALASKA",
    "HI": "HAWAII",
    "PR": "PUERTO_RICO",
}


def normalize_state_code(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def get_region_from_state(state_code) -> str:
    return REGION_BY_STATE.get(normalize_state_code(state_code), "CONUS")


def get_basic_rasters_for_region(project_root: Path, region: str):
    project_root = Path(project_root)
    region = str(region).strip().upper()

    if region == "ALASKA":
        base = project_root / "data" / "EF5-oCONUS-Parameters" / "basic"
        return (
            base / "ak_fdir_030218.tif",
            base / "ak_facc_030218.tif",
            base / "ak_dem_030218.tif",
        )

    if region == "HAWAII":
        base = project_root / "data" / "EF5-oCONUS-Parameters" / "basic"
        return (
            base / "hi_fdir_041818.tif",
            base / "hi_facc_041818.tif",
            base / "hi_dem_041818.tif",
        )

    if region == "PUERTO_RICO":
        base = project_root / "data" / "EF5-oCONUS-Parameters" / "basic"
        return (
            base / "carib_fdir_102016.tif",
            base / "carib_facc_102016.tif",
            base / "carib_dem_102016.tif",
        )

    base = project_root / "data" / "EF5_US_Params" / "basic"
    return (
        base / "fdir_usa.tif",
        base / "facc_usa.tif",
        base / "dem_usa.tif",
    )


def get_region_parameter_config(region: str) -> dict:
    region = str(region).strip().upper()

    if region == "ALASKA":
        return {
            "pet_loc": "data/EF5-oCONUS-Parameters/pet/",
            "pet_name": "FAO.PET.MM.tif",
            "crest_wm": "data/EF5-oCONUS-Parameters/parameters/CREST/wm_alaska_20190308c.tif",
            "crest_b": "data/EF5-oCONUS-Parameters/parameters/CREST/b_alaska_20190308c.tif",
            "crest_im": "data/EF5-oCONUS-Parameters/parameters/CREST/im_alaska_20190308c.tif",
            "crest_fc": "data/EF5-oCONUS-Parameters/parameters/CREST/ksat_alaska_20190308c.tif",
            "kw_under_grid": "",
            "kw_leaki_grid": "",
            "kw_alpha": "data/EF5-oCONUS-Parameters/parameters/KW/alpha_alaska_20190308c.tif",
            "kw_beta": "data/EF5-oCONUS-Parameters/parameters/KW/beta_alaska_20190308c.tif",
            "kw_alpha0": "data/EF5-oCONUS-Parameters/parameters/KW/alpha0_alaska_20190308c.tif",
        }

    if region == "HAWAII":
        return {
            "pet_loc": "data/EF5-oCONUS-Parameters/pet/",
            "pet_name": "FAO.PET.MM.tif",
            "crest_wm": "data/EF5-oCONUS-Parameters/parameters/CREST/wm_hawaii_20190304c.tif",
            "crest_b": "data/EF5-oCONUS-Parameters/parameters/CREST/b_hawaii_20190304c.tif",
            "crest_im": "data/EF5-oCONUS-Parameters/parameters/CREST/im_hawaii_20190304c.tif",
            "crest_fc": "data/EF5-oCONUS-Parameters/parameters/CREST/ksat_hawaii_20190304c.tif",
            "kw_under_grid": "",
            "kw_leaki_grid": "",
            "kw_alpha": "data/EF5-oCONUS-Parameters/parameters/KW/alpha_hawaii_20190304c.tif",
            "kw_beta": "data/EF5-oCONUS-Parameters/parameters/KW/beta_hawaii_20190304c.tif",
            "kw_alpha0": "data/EF5-oCONUS-Parameters/parameters/KW/alpha0_hawaii_20190304c.tif",
        }

    if region == "PUERTO_RICO":
        return {
            "pet_loc": "data/EF5-oCONUS-Parameters/pet/",
            "pet_name": "FAO.PET.MM.tif",
            "crest_wm": "data/EF5-oCONUS-Parameters/parameters/CREST/wm_carib_20190328c.tif",
            "crest_b": "data/EF5-oCONUS-Parameters/parameters/CREST/b_carib_20190328c.tif",
            "crest_im": "data/EF5-oCONUS-Parameters/parameters/CREST/im_carib_20190328c.tif",
            "crest_fc": "data/EF5-oCONUS-Parameters/parameters/CREST/ksat_carib_20190328c.tif",
            "kw_under_grid": "",
            "kw_leaki_grid": "",
            "kw_alpha": "data/EF5-oCONUS-Parameters/parameters/KW/alpha_carib_20190328c.tif",
            "kw_beta": "data/EF5-oCONUS-Parameters/parameters/KW/beta_carib_20190328c.tif",
            "kw_alpha0": "data/EF5-oCONUS-Parameters/parameters/KW/alpha0_carib_20190328c.tif",
        }

    return {
        "pet_loc": "Forcings/PET/",
        "pet_name": "PET_MM_usa.tif",
        "crest_wm": "data/EF5_US_Params/crest_params/wm_usa.tif",
        "crest_b": "data/EF5_US_Params/crest_params/b_usa.tif",
        "crest_im": "data/EF5_US_Params/crest_params/im_usa.tif",
        "crest_fc": "data/EF5_US_Params/crest_params/ksat_usa.tif",
        "kw_under_grid": "data/EF5_US_Params/kw_params/ksat_usa.tif",
        "kw_leaki_grid": "data/EF5_US_Params/kw_params/leaki_usa.tif",
        "kw_alpha": "data/EF5_US_Params/kw_params/alpha_usa.tif",
        "kw_beta": "data/EF5_US_Params/kw_params/beta_usa.tif",
        "kw_alpha0": "data/EF5_US_Params/kw_params/alpha0_usa.tif",
    }


@lru_cache(maxsize=1)
def load_basin_lookup_data():
    """
    Load and cache basin parquet file for fast lookups.
    
    Returns
    -------
    GeoDataFrame
        Basin geodataframe with polygon geometries and area_km2 attribute
    """
    project_root = Path.cwd()
    
    basin_path = project_root / "data" / "basin_delineations" / "flash_flood_protocol_basins.parquet"
    
    if not basin_path.exists():
        raise FileNotFoundError(f"Basin parquet not found: {basin_path}")
    
    basin_gdf = gpd.read_parquet(basin_path)
    
    return basin_gdf


def delineate_basin_from_gage(gage_id, out_dir):
    """
    Load watershed basin boundary from pre-computed GeoParquet file.
    
    Uses flash_flood_protocol_basins.parquet as the basin geometry source,
    avoiding external basin delineation API calls. Basin area is extracted from the
    area_km2 column in the parquet file.
    
    Parameters
    ----------
    gage_id : str or int
        USGS stream gage site number (e.g., '01234567' or 'usgs-01234567')
    out_dir : str or Path
        Directory to save basin boundary GeoJSON files
        
    Returns
    -------
    tuple
        (basin_gdf, site_gdf, basin_area_sqkm, output_paths)
        - basin_gdf: GeoDataFrame with basin polygon geometry
        - site_gdf: GeoDataFrame with gage point geometry  
        - basin_area_sqkm: Basin drainage area in square kilometers
        - output_paths: Dict with 'basin' and 'gage' file paths
        
    Raises
    ------
    ValueError
        If basin not found in parquet or basin geometry is invalid
        If existing basin/gage files are found but empty
    """
    gage_id = str(gage_id).strip()
    gage_id_normalized = normalize_gage_id(gage_id)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    basin_path = out_dir / f"{gage_id_normalized}_basin.geojson"
    site_path = out_dir / f"{gage_id_normalized}_gage.geojson"

    # If files already exist, load them and extract needed values
    if basin_path.exists() and site_path.exists():
        basin_gdf = gpd.read_file(basin_path)
        site_gdf = gpd.read_file(site_path)

        if site_gdf.empty:
            raise ValueError(f"Gage file exists but is empty: {site_path}")
        if basin_gdf.empty:
            raise ValueError(f"Basin file exists but is empty: {basin_path}")

        # Extract gage coordinates from point geometry
        lon = float(site_gdf.geometry.x.iloc[0])
        lat = float(site_gdf.geometry.y.iloc[0])

        # Try to get area_km2 attribute if available, otherwise calculate
        if "area_km2" in basin_gdf.columns:
            basin_area_sqkm = float(basin_gdf["area_km2"].iloc[0])
        else:
            basin_area_sqkm = basin_gdf.to_crs("EPSG:5070").geometry.area.sum() / 1_000_000.0

        output_paths = {
            "basin": basin_path,
            "gage": site_path,
        }

        return basin_gdf, site_gdf, basin_area_sqkm, output_paths

    # Load cached basin parquet
    basin_gdf_full = load_basin_lookup_data()
    
    # Search for basin by normalized gage ID
    # Work on a local copy so thread workers do not mutate the shared cached object.
    basin_lookup = basin_gdf_full.copy()
    basin_lookup["id_normalized"] = basin_lookup["id"].map(normalize_gage_id)
    matching_basins = basin_lookup[basin_lookup["id_normalized"] == gage_id_normalized]
    
    if matching_basins.empty:
        raise ValueError(f"No basin found in parquet for gage {gage_id} (normalized: {gage_id_normalized})")
    
    # Take the first match
    basin_row = matching_basins.iloc[0]
    basin_gdf = gpd.GeoDataFrame([basin_row], crs=matching_basins.crs)
    
    # Extract area from parquet's area_km2 column
    if "area_km2" in basin_gdf.columns and pd.notna(basin_gdf["area_km2"].iloc[0]):
        basin_area_sqkm = float(basin_gdf["area_km2"].iloc[0])
    else:
        # Fallback: calculate area from geometry
        basin_area_sqkm = basin_gdf.to_crs("EPSG:5070").geometry.area.sum() / 1_000_000.0
    
    # Create gage point geometry (placeholder, real coordinates from FAM later)
    site_gdf = gpd.GeoDataFrame(
        [{"gage_id": gage_id_normalized}],
        geometry=gpd.points_from_xy([0.0], [0.0]),
        crs="EPSG:4326",
    )
    
    # Save outputs as GeoJSON for downstream compatibility
    basin_gdf.to_file(basin_path, driver="GeoJSON")
    site_gdf.to_file(site_path, driver="GeoJSON")

    output_paths = {
        "basin": basin_path,
        "gage": site_path,
    }

    return basin_gdf, site_gdf, basin_area_sqkm, output_paths


def _process_one_gage(gage_id, out_dir):
    gage_id = str(gage_id).strip()

    try:
        _, _, basin_area_sqkm, output_paths = delineate_basin_from_gage(
            gage_id=gage_id,
            out_dir=out_dir,
        )

        return {
            "gage_id": gage_id,
            "status": "success",
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


def delineate_basins_from_csv(max_workers=8, skip_gages=None):
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

    gages_df["gage_id"] = gages_df["gage_id"].map(lambda g: str(g).strip() if pd.notna(g) else pd.NA)
    gage_ids = [str(g).strip() for g in gages_df["gage_id"].dropna().tolist()]
    
    # Filter out gages that should be skipped
    if skip_gages:
        gage_ids = [g for g in gage_ids if g not in skip_gages]
        if gage_ids:
            print(f"Skipping {len(skip_gages)} gage(s) with existing output. Processing {len(gage_ids)} gage(s).")
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
                    f"area={result['basin_area_sqkm']:.3f} km^2"
                )
            else:
                print(f"Failed: {result['gage_id']} -> {result['error']}")

    results_df = pd.DataFrame(results).sort_values("gage_id")

    gage_meta = gages_df[["gage_id"]].copy()
    if "state" in gages_df.columns:
        gage_meta["state"] = gages_df["state"].map(normalize_state_code)
    else:
        gage_meta["state"] = pd.NA
    gage_meta["region"] = gage_meta["state"].map(get_region_from_state)

    results_df = results_df.merge(
        gage_meta.drop_duplicates(subset=["gage_id"]),
        on="gage_id",
        how="left",
    )

    results_df.to_csv(out_dir / "basin_delineation_summary.csv", index=False)

    return results_df

def clip_raster_to_basin(in_raster, basin_gdf, out_raster):
    """
    Clip a raster dataset to the extent of a basin boundary polygon.
    
    Uses rasterio.mask to extract raster cells that intersect the basin geometry,
    setting all cells outside the basin to NoData. Preserves the original raster's
    coordinate reference system, resolution, and data type.
    
    Parameters
    ----------
    in_raster : str or Path
        Path to input raster file (e.g., DEM, flow direction, flow accumulation)
    basin_gdf : GeoDataFrame
        Basin boundary geometry, will be reprojected to match raster CRS if needed
    out_raster : str or Path  
        Path for output clipped raster file
        
    Raises
    ------
    ValueError
        If no valid geometries found after reprojection to raster CRS
    rasterio.errors.CRSError
        If coordinate system transformation fails
    """
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

def clip_main_layers_for_one_basin(
    basin_file,
    flow_direction_raster,
    flow_accumulation_raster,
    dem_raster,
    output_dir,
    region="CONUS",
):
    """
    Clip three core geospatial layers (DEM, flow direction, flow accumulation) for a single basin.
    
    This function processes one basin at a time and is designed to be called in parallel
    using ProcessPoolExecutor. It clips the continental-scale raster datasets to the
    basin boundary and identifies the outlet coordinates for streamflow computation.
    
    Parameters
    ----------
    basin_file : str or Path
        Path to basin boundary GeoJSON file (e.g., '12345678_basin.geojson')
    flow_direction_raster : str or Path
        Path to continental flow direction raster (typically fdir_usa.tif)
    flow_accumulation_raster : str or Path  
        Path to continental flow accumulation raster (typically facc_usa.tif)
    dem_raster : str or Path
        Path to continental digital elevation model (typically dem_usa.tif)
    output_dir : str or Path
        Directory to save clipped raster files (typically BasicData/)
        
    Returns
    -------
    dict
        Success result containing:
        - gage_id: Extracted from basin filename
        - status: 'success' or 'failed'
        - basin_file: Path to source basin file
        - flow_direction, flow_accumulation, dem: Paths to clipped rasters
        - fam_value: Maximum flow accumulation value (basin outlet)
        - snapped_longitude, snapped_latitude: Outlet coordinates in EPSG:4326
        - fam_row, fam_col: Row/column indices of outlet in clipped raster
        
    Raises
    ------
    ValueError
        If basin file is empty or lacks coordinate reference system
    """
    basin_file = Path(basin_file)
    output_dir = Path(output_dir)

    # Extract gage ID from basin filename (removes '_basin.geojson' suffix)
    gage_id = basin_file.stem.replace("_basin", "")
    basin_gdf = gpd.read_file(basin_file)

    if basin_gdf.empty:
        raise ValueError(f"Basin file is empty: {basin_file}")

    if basin_gdf.crs is None:
        raise ValueError(f"Basin file has no CRS: {basin_file}")

    # Define output filenames with gage ID prefix for organization
    flow_dir_out = output_dir / f"{gage_id}_flow_direction.tif"
    flow_acc_out = output_dir / f"{gage_id}_flow_accumulation.tif"
    dem_out = output_dir / f"{gage_id}_dem.tif"

    # Clip all three raster layers to the basin boundary
    # These are the core geospatial inputs required by EF5 model
    clip_raster_to_basin(flow_direction_raster, basin_gdf, flow_dir_out)
    clip_raster_to_basin(flow_accumulation_raster, basin_gdf, flow_acc_out)  
    clip_raster_to_basin(dem_raster, basin_gdf, dem_out)

    # Find the basin outlet (point of maximum flow accumulation)
    # This becomes the computational location for streamflow output
    fam_info = get_max_fam_cell_coords(flow_acc_out)

    return {
        "gage_id": gage_id,
        "region": str(region).strip().upper(),
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
    """
    Clip geospatial layers for all basins in parallel and merge results with basin summary.
    
    Orchestrates the clipping of continental-scale DEM, flow direction, and flow accumulation
    rasters for all basins delineated in previous steps. Uses ProcessPoolExecutor for 
    CPU-intensive raster operations. Updates the basin delineation summary with outlet 
    coordinates needed for EF5 model configuration.
    
    Parameters
    ----------
    max_workers : int, default 4
        Number of parallel processes for raster clipping operations
        
    Returns
    -------
    DataFrame
        Updated basin delineation summary with additional columns:
        - fam_value: Maximum flow accumulation at basin outlet
        - snapped_longitude, snapped_latitude: Basin outlet coordinates
        - fam_row, fam_col: Outlet position in clipped raster grid
        
    Raises
    ------
    FileNotFoundError
        If no basin boundary files found or required input rasters missing
    """
    project_root = Path.cwd()

    basin_dir = project_root / "data" / "basin_delineations"
    output_dir = project_root / "BasicData"
    output_dir.mkdir(parents=True, exist_ok=True)

    basin_files = sorted(basin_dir.glob("*_basin.geojson"))

    if not basin_files:
        raise FileNotFoundError(f"No basin files found in {basin_dir}")

    basin_summary_csv = project_root / "data" / "basin_delineations" / "basin_delineation_summary.csv"
    region_lookup = {}
    if basin_summary_csv.exists():
        basin_summary_df = pd.read_csv(basin_summary_csv, dtype={"gage_id": str})

        if "state" in basin_summary_df.columns:
            basin_summary_df["state"] = basin_summary_df["state"].map(normalize_state_code)

        if "region" not in basin_summary_df.columns:
            if "state" in basin_summary_df.columns:
                basin_summary_df["region"] = basin_summary_df["state"].map(get_region_from_state)
            else:
                basin_summary_df["region"] = "CONUS"

        region_lookup = {
            str(row["gage_id"]).strip(): str(row.get("region", "CONUS")).strip().upper()
            for _, row in basin_summary_df.iterrows()
            if pd.notna(row.get("gage_id"))
        }

    results = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for basin_file in basin_files:
            gage_id = basin_file.stem.replace("_basin", "")
            region = region_lookup.get(gage_id, "CONUS")
            flow_direction_raster, flow_accumulation_raster, dem_raster = get_basic_rasters_for_region(project_root, region)

            futures[
                executor.submit(
                    clip_main_layers_for_one_basin,
                    basin_file,
                    flow_direction_raster,
                    flow_accumulation_raster,
                    dem_raster,
                    output_dir,
                    region,
                )
            ] = basin_file

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
                    "region": region_lookup.get(gage_id, "CONUS"),
                    "status": "failed",
                    "basin_file": str(basin_file),
                    "error": str(e),
                })
                print(f"Failed: {gage_id} -> {e}")

    results_df = pd.DataFrame(results).sort_values("gage_id")
    results_df.to_csv(output_dir / "main_layer_clipping_summary.csv", index=False)

    # Update basin delineation summary with snapped outlet coordinates
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

# Unit conversion constant: Cubic Feet per Second to Cubic Meters per Second
# Derived from: 1 ft = 0.3048 m, so 1 ft³ = (0.3048 m)³ = 0.028316846592 m³
CFS_TO_CMS = 0.028316846592


def parse_ef5_time(value: str) -> datetime:
    """
    Parse EF5-style datetime string to UTC datetime object.
    
    EF5 model uses YYYYMMDDHHMMSS format for temporal specifications.
    All times are assumed to be UTC for consistency across the workflow.
    
    Parameters
    ----------
    value : str
        Datetime string in format YYYYMMDDHHMMSS (e.g., '20230101000000')
        
    Returns
    -------
    datetime
        UTC datetime object with timezone information
        
    Examples
    --------
    >>> parse_ef5_time('20230615123000')
    datetime.datetime(2023, 6, 15, 12, 30, tzinfo=datetime.timezone.utc)
    """
    return datetime.strptime(str(value).strip(), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


def fetch_usgs_iv(gage_id: str, start_utc: datetime, end_utc: datetime):
    """
    Fetch USGS instantaneous values (IV) streamflow data via web API.
    
    Downloads observed streamflow from USGS National Water Information System (NWIS)
    Instantaneous Values web service. Retrieves discharge data (parameter code 00060)
    and converts from cubic feet per second to cubic meters per second for EF5 compatibility.
    
    Parameters
    ----------
    gage_id : str
        USGS stream gage site number (8-digit format, e.g., '01234567')
    start_utc : datetime
        Start time for data retrieval (UTC timezone aware)
    end_utc : datetime
        End time for data retrieval (UTC timezone aware)
        
    Returns
    -------
    list of tuple
        List of (datetime, flow_cms) pairs sorted by timestamp
        - datetime: UTC timestamp of observation
        - flow_cms: Streamflow in cubic meters per second
        
    Notes
    -----
    - Uses USGS Water Services REST API: https://waterservices.usgs.gov/nwis/iv/
    - Parameter code 00060 = Discharge, cubic feet per second
    - Conversion factor: 1 CFS = 0.028316846592 CMS
    - Skips missing values and invalid data points
    - Returns empty list if no data available for the specified period
    """
    # Build USGS IV API request parameters
    # siteStatus='all' includes inactive sites that may have historical data
    params = {
        "format": "json",
        "sites": gage_id,
        "parameterCd": "00060",    # Discharge parameter code
        "startDT": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),  # ISO 8601 UTC format
        "endDT": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "siteStatus": "all",      # Include both active and inactive sites
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

    # Parse individual data points and convert units
    points = []
    for row in values[0].get("value", []):
        dt_str = row.get("dateTime", "")
        val_str = row.get("value", "")

        # Skip missing or invalid data points
        if not dt_str or val_str in ("", None):
            continue

        # Parse ISO 8601 datetime string and convert to UTC
        # Replace 'Z' suffix with UTC offset for proper timezone handling
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)

        # Convert discharge value to float and handle parsing errors
        try:
            q_cfs = float(val_str)
        except ValueError:
            continue  # Skip non-numeric values

        # Convert from cubic feet per second to cubic meters per second
        q_cms = q_cfs * CFS_TO_CMS
        points.append((dt, q_cms))

    # Sort by timestamp and return chronologically ordered data
    points.sort(key=lambda x: x[0])
    return points


def write_usgs_csv(rows, out_csv: Path):
    """
    Write USGS streamflow observations to EF5-compatible CSV format.
    
    Creates a standardized CSV file with UTC timestamps and flow values in CMS
    for use as observed data in EF5 model validation and comparison.
    
    Parameters
    ----------
    rows : list of tuple
        List of (datetime, flow_cms) pairs from USGS data processing
    out_csv : Path
        Output CSV file path (will create parent directories if needed)
        
    Notes
    -----
    - Always overwrites existing files to ensure clean data
    - Uses standardized column headers: 'Date (UTC)', 'Q (cms)'
    - Timestamps formatted as YYYY-MM-DD HH:MM:SS for EF5 compatibility
    - Flow values formatted to 6 decimal places for precision
    """
    # Write USGS streamflow rows to CSV.
    # Overwrites existing file every run.
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Date (UTC)", "Q (cms)"])
        for dt, q in rows:
            writer.writerow([dt.strftime("%Y-%m-%d %H:%M:%S"), f"{q:.6f}"])


def fetch_usgs_for_one_gage(gage_id: str, time_begin: str, time_end: str, out_dir: Path):
    """
    Download and process USGS streamflow data for a single gage.
    
    Handles the complete pipeline for one gage: time parsing, API data retrieval,
    temporal filtering, and CSV file generation. Designed for parallel execution
    across multiple gages.
    
    Parameters
    ----------
    gage_id : str
        USGS stream gage site identifier
    time_begin : str
        Start time in EF5 format (YYYYMMDDHHMMSS)
    time_end : str  
        End time in EF5 format (YYYYMMDDHHMMSS)
    out_dir : Path
        Directory for saving observation CSV files
        
    Returns
    -------
    dict
        Processing result with status ('success' or 'failed'), gage_id,
        and either output file path or error message
        
    Raises
    ------
    ValueError
        If time_end <= time_begin or invalid datetime formats
    RuntimeError
        If no USGS data returned for the specified gage and time period
    """
    # Download USGS IV streamflow for one gage and write EF5-ready observation CSV.
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


def fetch_usgs_for_all_gages(time_begin: str, time_end: str, max_workers=8, skip_gages=None):
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
    
    # Filter out gages that should be skipped
    if skip_gages:
        gage_ids = [g for g in gage_ids if g not in skip_gages]
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
    """
    Convert EF5 datetime format to control file time format.
    
    EF5 control files use a slightly different datetime format than the main
    workflow - they omit seconds and use YYYYMMDDHHMM instead of YYYYMMDDHHMMSS.
    
    Parameters
    ----------
    value : str
        Input datetime string (YYYYMMDDHHMMSS or YYYYMMDDHHMM)
        
    Returns
    -------
    str
        Control file compatible datetime string (YYYYMMDDHHMM)
        
    Raises
    ------
    ValueError
        If input format doesn't match expected EF5 datetime patterns
        
    Examples
    --------
    >>> ef5_datetime_to_control_time('20230615123045')
    '202306151230'
    >>> ef5_datetime_to_control_time('202306151230')
    '202306151230'
    """
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
    """
    Validate and normalize hydrological model name.
    
    Ensures model names are consistently formatted in uppercase and validates
    against supported EF5 model types implemented in this workflow.
    
    Parameters
    ----------
    model_to_run : str
        Model name (case-insensitive: 'crest', 'sac', 'hp')
        
    Returns
    -------
    str
        Normalized uppercase model name ('CREST', 'SAC', or 'HP')
        
    Raises
    ------
    ValueError
        If model name is not one of the supported options
        
    Notes
    -----
    Supported models:
    - CREST: Coupled Routing and Excess Storage model
    - SAC: Sacramento Soil Moisture Accounting model (SAC-SMA)
    - HP: Hydrologic Prediction model (simplified)
    """
    model = str(model_to_run).strip().upper()

    if model not in {"CREST", "SAC", "HP"}:
        raise ValueError("model_to_run must be one of: CREST, SAC, HP")

    return model


def build_precip_block(freq: str, region: str = "CONUS") -> tuple[str, str]:
    """
    Generate EF5 precipitation forcing configuration block based on temporal frequency.
    
    Configures either 2-minute MRMS GRIB2 data or hourly QPE data depending on the
    modeling time step. The precipitation forcing section tells EF5 where to find
    rainfall inputs and how to interpret the file naming convention.
    
    Parameters
    ---------- 
    freq : str
        Time frequency: '2u' for 2-minute, '1h' for hourly
    region : str
        Region key used for hourly precipitation folder selection
        
    Returns
    -------
    tuple
        (precip_block, precip_name) where:
        - precip_block: Multi-line string with EF5 PrecipForcing section
        - precip_name: Forcing name identifier for use in task configuration
        
    Notes
    -----
    - 2-minute data uses MRMS GRIB2 format from Forcings/Precipitation/2min/
    - Hourly data uses compressed GRIB2 from Forcings/Precipitation/hourly/{CONUS,AK,HI,PR}
    - File naming patterns use EF5 datetime placeholders (YYYYMMDD-HHUU00)
    """
    # Return:
    # - precip block text
    # - precip forcing name to use in task blocks
    freq = str(freq).strip()
    region = str(region).strip().upper()
    hourly_folder_by_region = {
        "ALASKA": "AK",
        "HAWAII": "HI",
        "PUERTO_RICO": "PR",
        "CONUS": "CONUS",
    }
    hourly_folder = hourly_folder_by_region.get(region, "CONUS")

    if freq == "2u":
        precip_name = "MRMS_GRIB"
        precip_block = f"""[PrecipForcing {precip_name}]
TYPE=GRIB2
UNIT=mm/h
FREQ=2u
LOC=Forcings/Precipitation/2min/{hourly_folder}
NAME=PrecipRate_00.00_YYYYMMDD-HHUU00.grib2
"""
    else:
        precip_name = "MRMS"
        precip_block = f"""[PrecipForcing {precip_name}]
TYPE=GRIB2
UNIT=mm/h
FREQ={freq}
LOC=Forcings/Precipitation/hourly/{hourly_folder}
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
    region: str = "CONUS",
) -> str:
    """
    Generate complete EF5 control file text with all required configuration sections.
    
    This is the most complex function in the workflow, generating a comprehensive EF5
    control file that includes basic settings, forcing data paths, gauge definitions,
    basin boundaries, parameter sets for multiple models (CREST, SAC-SMA, HP), routing
    parameters, and task execution instructions.
    
    Parameters
    ----------
    gage_id : str
        USGS stream gage identifier (8-digit format)
    latitude : float
        Gage latitude in decimal degrees (EPSG:4326) 
    longitude : float
        Gage longitude in decimal degrees (EPSG:4326)
    basin_area_sqkm : float
        Basin drainage area in square kilometers
    time_begin : str
        Simulation start time in EF5 format (YYYYMMDDHHMMSS)
    time_end : str
        Simulation end time in EF5 format (YYYYMMDDHHMMSS)  
    freq : str
        Time step frequency ('1h' for hourly, '2u' for 2-minute)
    model_to_run : str
        Hydrological model to execute ('CREST', 'SAC', or 'HP')
        
    Returns
    -------
    str
        Complete multi-section EF5 control file text ready for file writing
        
    Notes
    -----
    The generated control file contains these major sections:
    - [Basic]: Computational domain and outlet specifications
    - [PrecipForcing]: Precipitation data source configuration
    - [PETForcing]: Potential evapotranspiration data configuration  
    - [Gauge]: Stream gage location and metadata
    - [Basin]: Watershed boundary definition
    - [ParamSet]: Model parameter grids for CREST, SAC-SMA, HP models
    - [kwparamset]: Kinematic wave routing parameters
    - [Task]: Model execution task for the specified model
    - [Execute]: Task execution sequence
    
    The function supports three hydrological models:
    - CREST: Coupled Routing and Excess Storage model
    - SAC-SMA: Sacramento Soil Moisture Accounting model  
    - HP: Hydrologic Prediction model (simplified)
    
    All parameter grids reference continental-scale datasets in data/EF5_US_Params/
    """
    gage_id = str(gage_id).strip()
    region = str(region).strip().upper()
    model_to_run = normalize_model_name(model_to_run)
    region_cfg = get_region_parameter_config(region)

    # Convert EF5 datetime format to control file format (drops seconds)
    # EF5 control files use YYYYMMDDHHMM instead of YYYYMMDDHHMMSS
    time_begin_ctrl = ef5_datetime_to_control_time(time_begin)
    time_end_ctrl = ef5_datetime_to_control_time(time_end)

    # Map model names to their corresponding task identifiers
    # Each model gets a unique task name for execution targeting
    execute_task_lookup = {
        "CREST": f"Run{gage_id}crest",
        "SAC": f"Run{gage_id}sac", 
        "HP": f"Run{gage_id}hp",
    }
    execute_task = execute_task_lookup[model_to_run]

    basin_area_for_control = int(round(float(basin_area_sqkm)))

    precip_block, precip_name = build_precip_block(freq, region=region)

    crest_output_folder = f"Output/{gage_id}/crest/"
    sac_output_folder = f"Output/{gage_id}/sac/"
    hp_output_folder = f"Output/{gage_id}/hp/"

    crest_states_folder = f"states/{gage_id}/crest/"
    sac_states_folder = f"states/{gage_id}/sac/"
    hp_states_folder = f"states/{gage_id}/hp/"

    kw_grid_lines = ""
    if region_cfg["kw_under_grid"]:
        kw_grid_lines += f"under_grid={region_cfg['kw_under_grid']}\n"
    if region_cfg["kw_leaki_grid"]:
        kw_grid_lines += f"leaki_grid={region_cfg['kw_leaki_grid']}\n"
    kw_grid_lines += f"alpha_grid={region_cfg['kw_alpha']}\n"
    kw_grid_lines += f"beta_grid={region_cfg['kw_beta']}\n"
    kw_grid_lines += f"alpha0_grid={region_cfg['kw_alpha0']}\n"

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
LOC={region_cfg['pet_loc']}
NAME={region_cfg['pet_name']}

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
wm_grid={region_cfg['crest_wm']}
b_grid={region_cfg['crest_b']}
im_grid={region_cfg['crest_im']}
fc_grid={region_cfg['crest_fc']}
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
{kw_grid_lines}gauge={gage_id}
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
        region = str(row.get("region", "CONUS")).strip().upper()
        if not region:
            region = "CONUS"

        if region != "CONUS" and normalize_model_name(model_to_run) != "CREST":
            raise ValueError("CREST only available model outside CONUS")

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
            region=region,
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
            "region": region,
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
    skip_gages=None,
):
    project_root = Path.cwd()

    gage_csv = project_root / "gages" / "gage_ids.csv"
    summary_csv = project_root / "data" / "basin_delineations" / "basin_delineation_summary.csv"
    out_dir = project_root / "Control_Files"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not gage_csv.exists():
        raise FileNotFoundError(f"Gage CSV not found: {gage_csv}")

    if not summary_csv.exists():
        raise FileNotFoundError(f"Basin summary CSV not found: {summary_csv}")

    gages_df = pd.read_csv(gage_csv, dtype={"gage_id": str})
    summary_df = pd.read_csv(summary_csv, dtype={"gage_id": str})

    gages_df["gage_id"] = gages_df["gage_id"].map(lambda g: str(g).strip() if pd.notna(g) else pd.NA)
    if "state" in gages_df.columns:
        gages_df["state"] = gages_df["state"].map(normalize_state_code)
    else:
        gages_df["state"] = pd.NA
    gages_df["region"] = gages_df["state"].map(get_region_from_state)
    
    # Filter out gages that should be skipped
    if skip_gages:
        gages_df = gages_df[~gages_df["gage_id"].isin(skip_gages)]

    for col in ["state", "region"]:
        if col in summary_df.columns:
            summary_df = summary_df.drop(columns=[col])

    summary_df = summary_df.merge(
        gages_df[["gage_id", "state", "region"]].drop_duplicates(subset=["gage_id"]),
        on="gage_id",
        how="left",
    )

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
    results_df.to_csv(out_dir / "control_file_creation_summary.csv", index=False)

    return results_df
def run_ef5_for_one_control(control_file: Path, project_root: Path):
    control_file = Path(control_file)
    project_root = Path(project_root)
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
    log_file = control_file.parent / f"ef5_run_{gage_id}.log"

    try:
        with log_file.open("w", encoding="utf-8") as log:
            control_arg = str(control_file.relative_to(project_root))
            result = subprocess.run(
                ["./ef5", control_arg],
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


def run_ef5_for_all_controls(max_workers: int = 4, skip_gages=None):
    project_root = Path.cwd()
    control_files_dir = project_root / "Control_Files"
    control_files = sorted(control_files_dir.glob("control_*.txt"))

    # Filter out control files for gages that should be skipped
    if skip_gages:
        skip_set = {str(g).strip() for g in skip_gages}
        control_files = [
            cf for cf in control_files 
            if cf.stem.replace("control_", "") not in skip_set
        ]

    if not control_files:
        raise FileNotFoundError(f"No control files found in {control_files_dir}")

    ef5_exe = project_root / "ef5"
    if not ef5_exe.exists():
        raise FileNotFoundError(f"EF5 executable not found: {ef5_exe}")

    results = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_control = {
            executor.submit(run_ef5_for_one_control, control_file, project_root): control_file
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
    control_files_dir = project_root / "Control_Files"
    results_df.to_csv(control_files_dir / "ef5_execution_summary.csv", index=False)

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

def find_gages_with_output():
    """
    Identify USGS gage IDs that already have Output/ directories.
    
    Returns a set of gage IDs that should be skipped to avoid re-processing.
    """
    project_root = Path.cwd()
    output_dir = project_root / "Output"
    
    if not output_dir.exists():
        return set()
    
    return {gage_dir.name for gage_dir in output_dir.iterdir() if gage_dir.is_dir()}


def ensure_required_directories(project_root: Path):
    """
    Create the directory structure required by this workflow.

    This only creates folders and does not create any input files.
    """
    project_root = Path(project_root)

    required_dirs = [
        project_root / "Control_Files",
        project_root / "gages",
        project_root / "data",
        project_root / "data" / "basin_delineations",
        project_root / "data" / "EF5_US_Params",
        project_root / "data" / "EF5_US_Params" / "basic",
        project_root / "data" / "EF5_US_Params" / "crest_params",
        project_root / "data" / "EF5_US_Params" / "sac_params",
        project_root / "data" / "EF5_US_Params" / "kw_params",
        project_root / "Forcings",
        project_root / "Forcings" / "Precipitation",
        project_root / "Forcings" / "Precipitation" / "2min",
        project_root / "Forcings" / "Precipitation" / "2min" / "CONUS",
        project_root / "Forcings" / "Precipitation" / "2min" / "AK",
        project_root / "Forcings" / "Precipitation" / "2min" / "HI",
        project_root / "Forcings" / "Precipitation" / "2min" / "PR",
        project_root / "Forcings" / "Precipitation" / "hourly",
        project_root / "Forcings" / "Precipitation" / "hourly" / "CONUS",
        project_root / "Forcings" / "Precipitation" / "hourly" / "AK",
        project_root / "Forcings" / "Precipitation" / "hourly" / "HI",
        project_root / "Forcings" / "Precipitation" / "hourly" / "PR",
        project_root / "Forcings" / "PET",
        project_root / "BasicData",
        project_root / "observations",
        project_root / "Output",
        project_root / "states",
    ]

    created_count = 0
    for directory in required_dirs:
        if not directory.exists():
            created_count += 1
        directory.mkdir(parents=True, exist_ok=True)

    print(f"Directory setup complete. Ensured {len(required_dirs)} folders ({created_count} newly created).")


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
    """
    Execute the complete 6-step EF5 hydrological modeling workflow.
    
    This is the master orchestration function that sequences all workflow steps from
    basin delineation through model execution to visualization. Each step processes
    multiple stream gages in parallel and generates comprehensive summary reports.
    
    === WORKFLOW SEQUENCE ===
    
    Step 1: Basin Delineation
        - Reads gage IDs from gages/gage_ids.csv
        - Loads pre-computed basin polygons from flash_flood_protocol_basins.parquet
        - Matches basins by normalized gage ID
        - Saves basin boundaries as GeoJSON files
        - Creates basin_delineation_summary.csv
        
    Step 2: Raster Clipping 
        - Clips continental DEM, flow direction, flow accumulation rasters
        - Identifies basin outlet coordinates (max flow accumulation)
        - Saves clipped rasters to BasicData/ directory
        - Creates main_layer_clipping_summary.csv
        
    Step 3: USGS Data Download
        - Downloads observed streamflow from USGS IV API
        - Converts units from CFS to CMS
        - Filters data to simulation time period
        - Saves observations as CSV files in observations/
        - Creates usgs_download_summary.csv
        
    Step 4: Control File Creation
        - Merges basin, raster, and observation results
        - Generates EF5 model configuration files
        - Creates control_*.txt files with model parameters
        - Creates control_file_creation_summary.csv
        
    Step 5: EF5 Model Execution
        - Runs EF5 executable subprocess for each control file
        - Captures stdout/stderr logs for debugging
        - Saves model outputs to Output/ and states/ directories
        - Creates ef5_execution_summary.csv
        
    Step 6: Visualization (Optional)
        - Creates interactive Plotly HTML plots
        - Compares observed vs. modeled streamflow
        - Generates plots only for successful model runs
        - Creates plot_creation_summary.csv
    
    Parameters
    ----------
    time_begin : str
        Simulation start time in EF5 format (YYYYMMDDHHMMSS)
    time_end : str
        Simulation end time in EF5 format (YYYYMMDDHHMMSS)
    model_to_run : str
        Hydrological model to execute ('CREST', 'SAC', or 'HP')
    freq : str, default '1h'
        Temporal resolution ('1h' for hourly, '2u' for 2-minute)
    basin_workers : int, default 8
        Thread pool size for basin delineation (local lookup + GeoJSON writes)
    clip_workers : int, default 4
        Process pool size for raster clipping (CPU-bound geospatial operations)
    usgs_workers : int, default 8  
        Thread pool size for USGS data downloads (I/O-bound HTTP requests)
    control_workers : int, default 8
        Thread pool size for control file generation (I/O-bound file writes)
    ef5_workers : int, default 4
        Process pool size for EF5 model execution (subprocess-bound)
    plot_workers : int, default 4
        Thread pool size for plot generation (I/O-bound file operations)
    create_plots : bool, default True
        Whether to generate interactive visualizations (requires Plotly)
        
    Returns
    -------
    dict
        Comprehensive results summary containing DataFrames from all workflow steps:
        - 'basins': Basin delineation outcomes
        - 'clipping': Raster clipping outcomes
        - 'usgs': USGS data download outcomes
        - 'control_files': Control file creation outcomes
        - 'ef5_runs': Model execution outcomes
        - 'plots': Visualization creation outcomes (if enabled)
        
    Notes
    -----
    - Each step generates a summary CSV file for troubleshooting
    - Worker counts can be tuned based on system resources and API rate limits
    - Failed gages at any step are tracked but don't stop the overall workflow
    - All coordinate transformations use EPSG:4326 (WGS84) for consistency
    - Model outputs are organized by gage_id and model type for easy access
    
    Raises
    ------
    FileNotFoundError
        If required input files (gages/gage_ids.csv, EF5 executable) are missing
    ValueError
        If time_begin >= time_end or model name is invalid
    """
    project_root = Path.cwd()
    ensure_required_directories(project_root)

    # Identify gages with existing outputs and skip them
    skip_gages = find_gages_with_output()
    if skip_gages:
        print(f"Found existing outputs for {len(skip_gages)} gage(s): {sorted(skip_gages)}")
        print("These gages will be skipped.\n")
    
    # Step 1: Delineate watershed boundaries for all gages
    print("\n--- Step 1: Delineating basins ---")
    basin_results = delineate_basins_from_csv(max_workers=basin_workers, skip_gages=skip_gages)

    # Step 2: Clip continental rasters to basin boundaries
    print("\n--- Step 2: Clipping main raster layers ---")
    clip_results = clip_main_layers_for_all_basins(max_workers=clip_workers)

    # Step 3: Download observed streamflow data from USGS
    print("\n--- Step 3: Downloading USGS observations ---")
    usgs_results = fetch_usgs_for_all_gages(
        time_begin=time_begin,
        time_end=time_end,
        max_workers=usgs_workers,
        skip_gages=skip_gages,
    )

    # Step 4: Generate EF5 model configuration files
    print("\n--- Step 4: Creating control files ---")
    control_results = create_control_files_for_all_gages(
        time_begin=time_begin,
        time_end=time_end,
        model_to_run=model_to_run,
        freq=freq,
        max_workers=control_workers,
        skip_gages=skip_gages,
    )

    # Step 5: Execute EF5 hydrological model simulations
    print("\n--- Step 5: Running EF5 executable ---")
    ef5_results = run_ef5_for_all_controls(max_workers=ef5_workers, skip_gages=skip_gages)

    # Step 6: Create interactive plots (conditional based on success and availability)
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


if __name__ == "__main__":
    main()