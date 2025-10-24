#!/bin/bash

set -euo pipefail

DATASET="$1"

if [[ -z "$DATASET" ]]; then
  echo "Usage: $0 <dataset>"
  echo "Error: Please provide the dataset name."
  exit 1
fi

export_process_file() {
    FILE="$1"
    DATASET="$2"

    BASENAME=$(basename "$FILE" .zip)
    
    echo "Starting processing for $FILE"
    TMPDIR=$(mktemp -d "/tmp/unzip-${BASENAME}-XXXXXX")

    trap 'echo "Cleaning up $TMPDIR"; rm -rf "$TMPDIR"' EXIT

    unzip -o "$FILE" -d "$TMPDIR"

    INPUT=$(find "$TMPDIR" -maxdepth 1 -name "*.img" | head -1)

    if [[ -z "$INPUT" ]]; then
        echo "Error: No .img file found in $FILE"
        return 0
    fi

    echo "Processing $INPUT"
    
    TMP_TIF="${INPUT%.img}.tmp.tif"
    FINAL_TIF="${INPUT%.img}.tif"

    gdal_translate -of GTiff "$INPUT" "$TMP_TIF"

    gdalwarp -t_srs EPSG:3857 "$TMP_TIF" "$FINAL_TIF"

    OUTPUT_DIR="./tiff/$DATASET"
    mkdir -p "$OUTPUT_DIR"

    DEST_PATH="$OUTPUT_DIR/${BASENAME}.tif"

    mv "$FINAL_TIF" "$DEST_PATH"

    echo "Finished processing $FILE. Output at $DEST_PATH"
}

export -f export_process_file

INPUT_DIR="./raw/$DATASET"
OUTPUT_DIR_FINAL="./tiff/$DATASET"
FINAL_MERGED_TIF="./tiff/${DATASET}.tif"
FINAL_COG_TIF="./tiff/${DATASET}.cog.tif"
FILE_LIST_TXT="./tiff/$DATASET.txt"

if [[ ! -d "$INPUT_DIR" ]]; then
    echo "Error: Input directory $INPUT_DIR does not exist."
    exit 1
fi

echo "Starting parallel processing of files in $INPUT_DIR..."

find "$INPUT_DIR" -maxdepth 1 -name "*.zip" | parallel export_process_file {} "$DATASET"

echo "Parallel processing complete."

echo "Creating file list at $FILE_LIST_TXT"
mkdir -p "$OUTPUT_DIR_FINAL"
ls -1 "$OUTPUT_DIR_FINAL"/*.tif > "$FILE_LIST_TXT"

if [[ ! -s "$FILE_LIST_TXT" ]]; then
    echo "Error: No .tif files were generated. Cannot merge."
    exit 1
fi

echo "Merging all .tif files into $FINAL_MERGED_TIF..."
gdal_merge.py -o "$FINAL_MERGED_TIF" --optfile "$FILE_LIST_TXT"

gdal_translate "${FINAL_MERGED_TIF}" "${FINAL_COG_TIF}" -of COG -co COMPRESS=LZW -co NUM_THREADS=ALL_CPUS -co BLOCKSIZE=256 -co BIGTIFF=IF_SAFER -a_nodata 0

echo "All steps complete. Final merged file is at $FINAL_COG_TIF"

