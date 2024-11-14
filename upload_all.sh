#!/bin/bash

# Define the URL endpoint
URL="http://localhost:8983/solr/test/directupdate?commit=true"

# Define the directory containing files to upload

DIRECTORY="/data/javabin"
#install httpie
# Loop through each file in the directory and post it in the background
for FILE in "$DIRECTORY"/*; do
    if [ -f "$FILE" ]; then  # Check if it's a file
        echo "Uploading $FILE..."
        http --ignore-stdin POST "$URL" Content-Type:application/javabin @"$FILE" &
    fi
done

wait

# Wait for all background processes to finish
echo "All files in the directory uploaded."