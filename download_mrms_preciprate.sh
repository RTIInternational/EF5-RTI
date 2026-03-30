#!/usr/bin/env bash
set -euo pipefail

DEST_DIR_DEFAULT="/c/Repos/EF5-RTI/Precipitation"
START_DATE_DEFAULT="2025-05-12"
END_DATE_DEFAULT="2025-05-15"
ARCHIVE_BASE_URL="https://mtarchive.geol.iastate.edu"

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Download MRMS PrecipRate .gz files from mtarchive and decompress them.

Options:
  -s, --start-date YYYY-MM-DD   Start date (inclusive). Default: ${START_DATE_DEFAULT}
  -e, --end-date YYYY-MM-DD     End date (inclusive). Default: ${END_DATE_DEFAULT}
  -d, --dest-dir PATH           Destination directory. Default: ${DEST_DIR_DEFAULT}
  -n, --dry-run                 Show what would be downloaded/skipped without changes
  -h, --help                    Show this help message
EOF
}

START_DATE="$START_DATE_DEFAULT"
END_DATE="$END_DATE_DEFAULT"
DEST_DIR="$DEST_DIR_DEFAULT"
DRY_RUN="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
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
    -n|--dry-run)
      DRY_RUN="true"
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
echo "Date range: ${START_DATE} to ${END_DATE}"
if [[ "$DRY_RUN" == "true" ]]; then
  echo "Dry-run mode enabled: no files will be downloaded or decompressed."
fi

current_epoch="$START_EPOCH"
skipped_count=0
downloaded_count=0
would_download_count=0
while (( current_epoch <= END_EPOCH )); do
  year=$(date -u -d "@${current_epoch}" +%Y)
  month=$(date -u -d "@${current_epoch}" +%m)
  day=$(date -u -d "@${current_epoch}" +%d)
  url="${ARCHIVE_BASE_URL}/${year}/${month}/${day}/mrms/ncep/PrecipRate/"
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
      wget --no-verbose --directory-prefix="$DEST_DIR" "${url}${remote_file}"
      ((downloaded_count+=1))
    fi
  done

  current_epoch=$(( current_epoch + 86400 ))
done

shopt -s nullglob
gz_files=("$DEST_DIR"/*.gz)

if [[ "$DRY_RUN" == "true" ]]; then
  echo "Summary: skipped=${skipped_count}, would-download=${would_download_count}, downloaded=0"
  echo "Dry-run complete. No changes made."
  exit 0
fi

if (( ${#gz_files[@]} == 0 )); then
  echo "No .gz files found to decompress in $DEST_DIR"
  exit 0
fi

echo "Decompressing ${#gz_files[@]} files..."
gunzip -f "${gz_files[@]}"

echo "Summary: skipped=${skipped_count}, downloaded=${downloaded_count}"
echo "Done. Files are available in: $DEST_DIR"
