#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
# Treat unset variables as an error.
# The return value of a pipeline is the status of the last command to exit with a
# non-zero status, or zero if no command exited with a non-zero status.
set -euo pipefail

DATASET="$1"

if [[ -z "$DATASET" ]]; then
  echo "Usage: $0 <dataset>"
  echo "Error: Please provide the dataset name."
  exit 1
fi

# Define the processing function
# This function will be called by GNU Parallel for each input file.
export_process_file() {
    FILE="$1"
    DATASET="$2"

    # Use %B to get the basename of the file without the extension
    BASENAME=$(basename "$FILE" .zip)
    
    echo "Starting processing for $FILE"

    # Create random directory in /tmp
    # We add the basename to make it more identifiable
    TMPDIR=$(mktemp -d "/tmp/unzip-${BASENAME}-XXXXXX")

    # Ensure cleanup happens even if the script fails mid-way
    # trap will execute the command when the function exits (EXIT)
    trap 'echo "Cleaning up $TMPDIR"; rm -rf "$TMPDIR"' EXIT

    # Write to tmp directory
    unzip -o "$FILE" -d "$TMPDIR"

    # Find the .img file. Using find is safer than ls | head
    # This handles cases where there might be other files.
    INPUT=$(find "$TMPDIR" -maxdepth 1 -name "*.img" | head -1)

    if [[ -z "$INPUT" ]]; then
        echo "Error: No .img file found in $FILE"
        # We 'return 0' instead of 'exit 1' to allow parallel to continue
        # with other files.
        return 0
    fi

    echo "Processing $INPUT"
    
    # Define output filenames
    TMP_TIF="${INPUT%.img}.tmp.tif"
    FINAL_TIF="${INPUT%.img}.tif"

    # Output to the same directory as the input file, but with .tif extension
    gdal_translate -of GTiff "$INPUT" "$TMP_TIF"

    # Warp the file
    gdalwarp -t_srs EPSG:3857 "$TMP_TIF" "$FINAL_TIF"

    # Create the final output directory
    OUTPUT_DIR="./tiff/$DATASET"
    mkdir -p "$OUTPUT_DIR"

    # Define the final destination path
    DEST_PATH="$OUTPUT_DIR/${BASENAME}.tif"

    # Move the final warped file to the destination
    mv "$FINAL_TIF" "$DEST_PATH"

    echo "Finished processing $FILE. Output at $DEST_PATH"
    
    # trap will automatically clean up $TMPDIR
}

# Export the function so 'parallel' can access it
export -f export_process_file

# --- Main Execution ---

INPUT_DIR="./raw/$DATASET"
OUTPUT_DIR_FINAL="./tiff/$DATASET"
FINAL_MERGED_TIF="./tiff/${DATASET}.tif"
FINAL_COG_TIF="./tiff/${DATASET}.cog.tif"
FILE_LIST_TXT="./tiff/$DATASET.txt"

# Check if input directory exists
if [[ ! -d "$INPUT_DIR" ]]; then
    echo "Error: Input directory $INPUT_DIR does not exist."
    exit 1
fi

echo "Starting parallel processing of files in $INPUT_DIR..."

# Use 'find' to get all .zip files and pipe them to 'parallel'
# {} is the placeholder for the input (the file path)
# We pass $DATASET as the second argument to the function
find "$INPUT_DIR" -maxdepth 1 -name "*.zip" | parallel export_process_file {} "$DATASET"

echo "Parallel processing complete."

# --- Merging Step ---

echo "Creating file list at $FILE_LIST_TXT"
# Ensure the output directory exists before writing the list
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

