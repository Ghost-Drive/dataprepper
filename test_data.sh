#!/bin/bash

if [ ! -d "test_data" ]; then
    mkdir test_data
fi

cd test_data

# Creating directories A, B, C, D, and E
for dir in A B C D E; do
    mkdir -p "$dir"

    # Generating a random number between 5 and 10 for file count
    # Set the total size for each directory to 100MB
    dirSize=104857600 # 100MB in bytes

    # Initialize the current size to 0
    currentSize=0

    while ((currentSize < dirSize)); do
        # Set file size to 1MB
        fileSize=1048576 # 1MB in bytes

        # Creating a dummy file of 1MB size
        dd if=/dev/urandom of="${dir}/$(date +%s%N).txt" bs=${fileSize} count=1 status=none
        # head -c "${fileSize}" /dev/urandom | base64 > "${dir}/$(date +%s%N).txt"

        # Update the current size
        currentSize=$((currentSize + fileSize))
    done
done

cd ..

echo "Folders and files created successfully."