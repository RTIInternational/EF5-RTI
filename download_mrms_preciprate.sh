#!/usr/bin/env bash
set -euo pipefail

DEST_DIR_DEFAULT="${HOME}/MRMS_preciprate"
CONUS_BASE_URL="https://mtarchive.geol.iastate.edu"
NON_CONUS_BASE_URL="https://noaa-mrms-pds.s3.amazonaws.com"
NON_CONUS_MIN_DATE="2020-10-15"
JOBS_DEFAULT="$(command -v nproc >/dev/null 2>&1 && nproc || echo 4)"

PRODUCT=""
PRODUCT_SUBDIR=""
REGION="all"

CONUS_PRODUCT_PATH=""
NON_CONUS_PRODUCT_PATH=""

declare -a REGIONS_TO_DOWNLOAD=()

normalize_region() {
  local input="$1"
  local upper
  upper=$(printf '%s' "$input" | tr '[:lower:]' '[:upper:]')
  case "$upper" in
    CONUS|HI|PR|AK|ALL)
      printf '%s\n' "$upper"
      ;;
    *)
      return 1
      ;;
  esac
}

resolve_regions() {
  local normalized="$1"
  if [[ "$normalized" == "ALL" ]]; then
    REGIONS_TO_DOWNLOAD=("CONUS" "HI" "PR" "AK")
  else
    REGIONS_TO_DOWNLOAD=("$normalized")
  fi
}

set_product_details() {
  case "$1" in
    2min)
      PRODUCT_SUBDIR="2min"
      CONUS_PRODUCT_PATH="PrecipRate"
      NON_CONUS_PRODUCT_PATH="PrecipRate_00.00"
      ;;
    hourly)
      PRODUCT_SUBDIR="hourly"
      CONUS_PRODUCT_PATH="RadarOnly_QPE_01H"
      NON_CONUS_PRODUCT_PATH="RadarOnly_QPE_01H_00.00"
      ;;
    *)
      echo "Error: Unsupported product '$1'. Use '2min' or 'hourly'."
      exit 1
      ;;
  esac
}

region_nest_name() {
  case "$1" in
    HI)
      printf '%s\n' "HAWAII"
      ;;
    PR)
      printf '%s\n' "CARIB"
      ;;
    AK)
      printf '%s\n' "ALASKA"
      ;;
    *)
      return 1
      ;;
  esac
}

build_region_listing_and_download_info() {
  local region="$1"
  local year="$2"
  local month="$3"
  local day="$4"

  if [[ "$region" == "CONUS" ]]; then
    local conus_url
    conus_url="${CONUS_BASE_URL}/${year}/${month}/${day}/mrms/ncep/${CONUS_PRODUCT_PATH}/"
    printf '%s\n%s\n' "$conus_url" "$conus_url"
    return 0
  fi

  local yyyymmdd nest prefix
  yyyymmdd="${year}${month}${day}"
  nest=$(region_nest_name "$region")
  prefix="${nest}/${NON_CONUS_PRODUCT_PATH}/${yyyymmdd}/"
  printf '%s\n%s\n' "${NON_CONUS_BASE_URL}/?list-type=2&prefix=${prefix}" "${NON_CONUS_BASE_URL}/"
}

is_hour_file() {
  local file="$1"
  local stamp
  stamp=$(basename "$file" | grep -Eo '[0-9]{8}-[0-9]{6}' | head -n1 || true)
  if [[ -z "$stamp" ]]; then
    return 1
  fi
  [[ "$stamp" =~ -[0-9]{2}0000$ ]]
}

fetch_remote_files() {
  local region="$1"
  local list_url="$2"
  local page

  if ! page=$(wget -qO- "$list_url"); then
    echo "Error: Failed to fetch listing from $list_url"
    return 1
  fi

  if [[ "$region" == "CONUS" ]]; then
    printf '%s\n' "$page" \
      | grep -Eo 'href="[^"]+\.gz"' \
      | sed -E 's/href="([^"]+)"/\1/' \
      | sed 's#^\./##' \
      | sort -u
  else
    printf '%s\n' "$page" \
      | grep -Eo '<Key>[^<]+\.gz</Key>' \
      | sed -E 's#<Key>([^<]+)</Key>#\1#' \
      | sort -u
  fi
}

run_parallel_downloads() {
  local dest="$1"
  shift
  local -a urls=("$@")

  if (( ${#urls[@]} == 0 )); then
    return 0
  fi

  printf '%s\n' "${urls[@]}" \
    | xargs -P "$JOBS" -I '{}' wget \
        --no-verbose \
        --continue \
        --tries=20 \
        --waitretry=5 \
        --timeout=30 \
        --read-timeout=30 \
        --retry-connrefused \
        --directory-prefix="$dest" '{}'
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Download MRMS precipitation .gz files from CONUS/non-CONUS sources and optionally decompress them.

Options:
  -p, --product 2min|hourly    Required. Choose 2-minute PrecipRate or hourly QPE (RadarOnly_QPE_01H)
  -r, --region CONUS|HI|PR|AK|all
                               Region(s) to download. Default: all
                               HI=Hawaii, PR=Puerto Rico, AK=Alaska
  -s, --start-date YYYY-MM-DD   Required. Start date (inclusive)
  -e, --end-date YYYY-MM-DD     Required. End date (inclusive)
  -d, --dest-dir PATH           Base destination directory. Default: ${DEST_DIR_DEFAULT}
                                Data are saved under PATH/<product>/<region>.
  -j, --jobs N                  Number of parallel download jobs. Default: ${JOBS_DEFAULT}
  -n, --dry-run                 Show what would be downloaded/skipped without changes
  -u, --unzip                   Decompress downloaded .gz files after downloading (default: false)
  -h, --help                    Show this help message

Examples:
  $(basename "$0") --product 2min --region CONUS -s 2022-07-27 -e 2022-07-30 -d ~/MRMS_preciprate
  $(basename "$0") --product hourly --region HI -s 2021-01-01 -e 2021-01-02 -d ~/MRMS_preciprate -j 8
  $(basename "$0") --product hourly --region all -s 2021-01-01 -e 2021-01-02 -d ~/MRMS_preciprate
EOF
}

START_DATE=""
END_DATE=""
DEST_DIR="$DEST_DIR_DEFAULT"
DRY_RUN="false"
UNZIP="false"
JOBS="$JOBS_DEFAULT"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--product)
      PRODUCT="${2:-}"
      shift 2
      ;;
    -r|--region)
      REGION="${2:-}"
      shift 2
      ;;
    -s|--start-date)
      START_DATE="${2:-}"
      shift 2
      ;;
    -e|--end-date)
      END_DATE="${2:-}"
      shift 2
      ;;
    -d|--dest-dir)
      DEST_DIR="${2:-}"
      shift 2
      ;;
    -j|--jobs)
      JOBS="${2:-}"
      shift 2
      ;;
    -n|--dry-run)
      DRY_RUN="true"
      shift
      ;;
    -u|--unzip)
      UNZIP="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: Unknown argument '$1'"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
  echo "Error: --start-date and --end-date are required."
  usage
  exit 1
fi

if [[ -z "$PRODUCT" ]]; then
  echo "Error: --product is required and must be either '2min' or 'hourly'."
  usage
  exit 1
fi

if ! REGION_NORMALIZED=$(normalize_region "$REGION"); then
  echo "Error: --region must be one of CONUS, HI, PR, AK, or all."
  usage
  exit 1
fi
resolve_regions "$REGION_NORMALIZED"

if ! [[ "$JOBS" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: --jobs must be a positive integer."
  exit 1
fi

set_product_details "$PRODUCT"
BASE_DEST_DIR="${DEST_DIR}/${PRODUCT_SUBDIR}"

if ! START_EPOCH=$(date -d "$START_DATE" +%s 2>/dev/null); then
  echo "Error: Invalid start date '$START_DATE'. Expected YYYY-MM-DD."
  exit 1
fi

if ! END_EPOCH=$(date -d "$END_DATE" +%s 2>/dev/null); then
  echo "Error: Invalid end date '$END_DATE'. Expected YYYY-MM-DD."
  exit 1
fi

if (( START_EPOCH > END_EPOCH )); then
  echo "Error: start-date must be earlier than or equal to end-date."
  exit 1
fi

if ! NON_CONUS_MIN_EPOCH=$(date -d "$NON_CONUS_MIN_DATE" +%s 2>/dev/null); then
  echo "Error: Failed to parse minimum non-CONUS date '${NON_CONUS_MIN_DATE}'."
  exit 1
fi

for selected_region in "${REGIONS_TO_DOWNLOAD[@]}"; do
  if [[ "$selected_region" != "CONUS" ]] && (( START_EPOCH < NON_CONUS_MIN_EPOCH )); then
    echo "Error: Region ${selected_region} data starts at ${NON_CONUS_MIN_DATE}. Use --start-date ${NON_CONUS_MIN_DATE} or later."
    exit 1
  fi
done

mkdir -p "$BASE_DEST_DIR"
cd "$BASE_DEST_DIR"

echo "Downloading .gz files under: $BASE_DEST_DIR"
echo "Product: ${PRODUCT} (CONUS=${CONUS_PRODUCT_PATH}, non-CONUS=${NON_CONUS_PRODUCT_PATH})"
echo "Regions: ${REGIONS_TO_DOWNLOAD[*]}"
echo "Date range: ${START_DATE} to ${END_DATE}"
echo "Parallel jobs: ${JOBS}"
if [[ "$DRY_RUN" == "true" ]]; then
  echo "Dry-run mode enabled: no files will be downloaded or decompressed."
fi
if [[ "$UNZIP" == "true" ]]; then
  echo "Unzip mode enabled: .gz files will be decompressed after downloading."
fi

current_epoch="$START_EPOCH"
skipped_count=0
downloaded_count=0
would_download_count=0

for region_name in "${REGIONS_TO_DOWNLOAD[@]}"; do
  region_dest_dir="${BASE_DEST_DIR}/${region_name}"
  mkdir -p "$region_dest_dir"

  current_epoch="$START_EPOCH"
  while (( current_epoch <= END_EPOCH )); do
    year=$(date -u -d "@${current_epoch}" +%Y)
    month=$(date -u -d "@${current_epoch}" +%m)
    day=$(date -u -d "@${current_epoch}" +%d)

    mapfile -t endpoint_info < <(build_region_listing_and_download_info "$region_name" "$year" "$month" "$day")
    list_url="${endpoint_info[0]}"
    download_base_url="${endpoint_info[1]}"

    echo "-> [${region_name}] Fetching from $list_url"

    mapfile -t remote_files < <(fetch_remote_files "$region_name" "$list_url")

    if (( ${#remote_files[@]} == 0 )); then
      echo "   [${region_name}] No .gz links found"
      current_epoch=$(( current_epoch + 86400 ))
      continue
    fi

    day_planned_count=0
    day_existing_count=0
    day_batch_urls=()

    for remote_file in "${remote_files[@]}"; do
      if [[ "$PRODUCT" == "hourly" ]] && ! is_hour_file "$remote_file"; then
        continue
      fi

      filename=$(basename "$remote_file")
      local_gz="$region_dest_dir/$filename"
      local_unzipped="${local_gz%.gz}"

      # Skip if either compressed or decompressed output already exists.
      if [[ -f "$local_gz" || -f "$local_unzipped" ]]; then
        echo "   [${region_name}] Skipping existing: $filename"
        ((skipped_count+=1))
        ((day_existing_count+=1))
        continue
      fi

      if [[ "$DRY_RUN" == "true" ]]; then
        ((would_download_count+=1))
      else
        full_url="${download_base_url}${remote_file}"
        day_batch_urls+=("$full_url")
        ((day_planned_count+=1))
      fi
    done

    if [[ "$DRY_RUN" == "false" ]] && (( day_planned_count > 0 )); then
      echo "   [${region_name}] Downloading ${day_planned_count} file(s) for ${year}-${month}-${day} using ${JOBS} parallel job(s)..."
      run_parallel_downloads "$region_dest_dir" "${day_batch_urls[@]}"
      ((downloaded_count+=day_planned_count))
    elif [[ "$DRY_RUN" == "false" ]] && (( day_existing_count > 0 )); then
      echo "   [${region_name}] All target files already exist for ${year}-${month}-${day}; skipping download step."
    fi

    current_epoch=$(( current_epoch + 86400 ))
  done
done

if [[ "$DRY_RUN" == "true" ]]; then
  echo "Summary: skipped=${skipped_count}, would-download=${would_download_count}, downloaded=0"
  echo "Dry-run complete. No changes made."
  exit 0
fi

echo "Summary: skipped=${skipped_count}, downloaded=${downloaded_count}"

if [[ "$UNZIP" == "true" ]]; then
  mapfile -t gz_files < <(find "$BASE_DEST_DIR" -type f -name '*.gz' | sort)
  if (( ${#gz_files[@]} == 0 )); then
    echo "No .gz files found to decompress in $BASE_DEST_DIR"
  else
    echo "Decompressing ${#gz_files[@]} files using ${JOBS} parallel job(s)..."
    printf '%s\n' "${gz_files[@]}" | xargs -n 1 -P "$JOBS" gunzip -f
  fi
fi

echo "Done. Files are available in: $BASE_DEST_DIR"
