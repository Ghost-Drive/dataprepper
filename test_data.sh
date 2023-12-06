#!/bin/bash

if [ ! -d "test_data" ]; then
    mkdir test_data
fi

cd test_data

# Creating directories A, B, C, D, and E
for dir in A B C D E; do
    mkdir -p "$dir"

    # Generating a random number between 5 and 10 for file count
    fileCount=$((RANDOM % 6 + 20))

    # Calculate the number of files needed to reach 17GB with each file being 1MB in size
    # Calculate the total size for each directory (between 4GB and 7GB)
    dirSize=$((RANDOM % 4 + 4 * 1024 * 1024))

    # Initialize the current size to 0
    currentSize=0

    while ((currentSize < dirSize)); do
        # Generate a random file size between 500KB and 10MB
        fileSize=$((RANDOM % 10 * 1024 + 500))

        # If adding another file of this size would exceed the total size, adjust the file size
        if ((currentSize + fileSize > dirSize)); then
            fileSize=$((dirSize - currentSize))
        fi

        # Creating a dummy file of the calculated size
        head -c "${fileSize}K" /dev/urandom | base64 > "${dir}/$(date +%s%N).txt"

        # Update the current size
        currentSize=$((currentSize + fileSize))
    done
done

cd ..

echo "Folders and files created successfully."