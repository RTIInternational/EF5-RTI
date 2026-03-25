#!/usr/bin/env bash
set -euo pipefail

DEST_DIR_DEFAULT="${HOME}/MRMS_preciprate"
START_DATE_DEFAULT="2022-07-27"
END_DATE_DEFAULT="2022-07-30"
ARCHIVE_BASE_URL="https://mtarchive.geol.iastate.edu"
JOBS_DEFAULT="$(command -v nproc >/dev/null 2>&1 && nproc || echo 4)"

PRODUCT=""
PRODUCT_PATH=""
PRODUCT_SUBDIR=""

set_product_details() {
  case "$1" in
    2min)
      PRODUCT_PATH="PrecipRate"
      PRODUCT_SUBDIR="2min"
      ;;
    hourly)
      PRODUCT_PATH="RadarOnly_QPE_01H"
      PRODUCT_SUBDIR="hourly"
      ;;
    *)
      echo "Error: Unsupported product '$1'. Use '2min' or 'hourly'."
      exit 1
      ;;
  esac
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

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Download MRMS PrecipRate .gz files from mtarchive and optionally decompress them.

Options:
  -p, --product 2min|hourly    Required. Choose 2-minute PrecipRate or hourly QPE (RadarOnly_QPE_01H)
  -s, --start-date YYYY-MM-DD   Start date (inclusive). Default: ${START_DATE_DEFAULT}
  -e, --end-date YYYY-MM-DD     End date (inclusive). Default: ${END_DATE_DEFAULT}
  -d, --dest-dir PATH           Base destination directory. Default: ${DEST_DIR_DEFAULT}
                                Data are saved under PATH/2min or PATH/hourly.
  -j, --jobs N                  Number of parallel download jobs. Default: ${JOBS_DEFAULT}
  -n, --dry-run                 Show what would be downloaded/skipped without changes
  -u, --unzip                   Decompress downloaded .gz files after downloading (default: false)
  -h, --help                    Show this help message

Examples:
  $(basename "$0") --product 2min -s 2022-07-27 -e 2022-07-30 -d ~/MRMS_preciprate
  $(basename "$0") --product hourly -s 2020-01-01 -e 2020-01-02 -d ~/MRMS_preciprate -j 8
EOF
}

START_DATE="$START_DATE_DEFAULT"
END_DATE="$END_DATE_DEFAULT"
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

if [[ -z "$START_DATE" || -z "$END_DATE" || -z "$DEST_DIR" ]]; then
  echo "Error: --start-date, --end-date, and --dest-dir require values."
  usage
  exit 1
fi

if [[ -z "$PRODUCT" ]]; then
  echo "Error: --product is required and must be either '2min' or 'hourly'."
  usage
  exit 1
fi

if ! [[ "$JOBS" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: --jobs must be a positive integer."
  exit 1
fi

set_product_details "$PRODUCT"
DEST_DIR="${DEST_DIR}/${PRODUCT_SUBDIR}"

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

mkdir -p "$DEST_DIR"
cd "$DEST_DIR"

echo "Downloading .gz files into: $DEST_DIR"
echo "Product: ${PRODUCT} (${PRODUCT_PATH})"
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
download_urls=()
while (( current_epoch <= END_EPOCH )); do
  year=$(date -u -d "@${current_epoch}" +%Y)
  month=$(date -u -d "@${current_epoch}" +%m)
  day=$(date -u -d "@${current_epoch}" +%d)
  url="${ARCHIVE_BASE_URL}/${year}/${month}/${day}/mrms/ncep/${PRODUCT_PATH}/"
  echo "-> Fetching from $url"

  mapfile -t remote_files < <(
    wget -qO- "$url" \
      | grep -Eo 'href="[^"]+\.gz"' \
      | sed -E 's/href="([^"]+)"/\1/' \
      | sed 's#^\./##' \
      | sort -u
  )

  if (( ${#remote_files[@]} == 0 )); then
    echo "   No .gz links found at $url"
    current_epoch=$(( current_epoch + 86400 ))
    continue
  fi

  for remote_file in "${remote_files[@]}"; do
    if [[ "$PRODUCT" == "hourly" ]] && ! is_hour_file "$remote_file"; then
      continue
    fi

    filename=$(basename "$remote_file")
    local_gz="$DEST_DIR/$filename"
    local_unzipped="${local_gz%.gz}"

    if [[ -f "$local_gz" || -f "$local_unzipped" ]]; then
      echo "   Skipping existing: $filename"
      ((skipped_count+=1))
      continue
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
      ((would_download_count+=1))
    else
      download_urls+=("${url}${remote_file}")
    fi
  done

  current_epoch=$(( current_epoch + 86400 ))
done

if [[ "$DRY_RUN" == "true" ]]; then
  echo "Summary: skipped=${skipped_count}, would-download=${would_download_count}, downloaded=0"
  echo "Dry-run complete. No changes made."
  exit 0
fi

if (( ${#download_urls[@]} > 0 )); then
  echo "Downloading ${#download_urls[@]} files using ${JOBS} parallel job(s)..."
  printf '%s\n' "${download_urls[@]}" \
    | xargs -P "$JOBS" -I '{}' wget --no-verbose --directory-prefix="$DEST_DIR" '{}'
  downloaded_count=${#download_urls[@]}
fi

echo "Summary: skipped=${skipped_count}, downloaded=${downloaded_count}"

if [[ "$UNZIP" == "true" ]]; then
  shopt -s nullglob
  gz_files=("$DEST_DIR"/*.gz)
  if (( ${#gz_files[@]} == 0 )); then
    echo "No .gz files found to decompress in $DEST_DIR"
  else
    echo "Decompressing ${#gz_files[@]} files using ${JOBS} parallel job(s)..."
    printf '%s\n' "${gz_files[@]}" | xargs -n 1 -P "$JOBS" gunzip -f
  fi
fi

echo "Done. Files are available in: $DEST_DIR"
