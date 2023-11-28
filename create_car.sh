#!/bin/bash

# Creating directories A, B, C, D, and E
for dir in A B C D E; do
    mkdir -p "$dir"

    # Generating a random number between 5 and 10 for file count
    fileCount=$((RANDOM % 6 + 20))

    for ((i=1; i<=fileCount; i++)); do
        # Creating a dummy file of 1 MB size
        head -c 1M /dev/urandom | base64 > "${dir}/${i}.txt"
    done
done

echo "Folders and files created successfully."