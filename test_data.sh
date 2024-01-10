#!/bin/bash

if [ ! -d "test_data" ]; then
    mkdir test_data
fi

cd test_data

totalSize=0
targetSize=$((20 * 1024 * 1024 * 1024)) # 20GB in bytes
folderCount=30
fileSize=$((2 * 1024 * 1024)) # 2MB in bytes
filesRemaining=$((targetSize / fileSize)) # Total number of files to be created

for ((i=1; i<=folderCount; i++)); do
    folderName="folder_$i"
    mkdir -p "$folderName"
    
    # Decide how many files to create in this folder
    if [ $i -lt $folderCount ]; then
        # Random number of files for this folder, but at least one file
        maxFilesForThisFolder=$((filesRemaining - (folderCount - i)))
        filesForThisFolder=$((RANDOM % maxFilesForThisFolder + 1))
    else
        # For the last folder, use all remaining files
        filesForThisFolder=$filesRemaining
    fi

    for ((j=1; j<=filesForThisFolder; j++)); do
        filename="$(date +%s%N).txt"
        dd if=/dev/urandom of="${folderName}/${filename}" bs=${fileSize} count=1 status=none
        totalSize=$((totalSize + fileSize))
    done

    # Update the remaining file count
    filesRemaining=$((filesRemaining - filesForThisFolder))
done

echo "Total size created: $totalSize bytes"



# Creating directories A, B, C, D, and E
# for dir in A B C D E; do
#     mkdir -p "$dir"

#     # Generating a random number between 5 and 10 for file count
#     # Set the total size for each directory to 100MB
#     dirSize=104857600 # 100MB in bytes

#     # Initialize the current size to 0
#     currentSize=0

#     while ((currentSize < dirSize)); do
#         # Set file size to 1MB
#         fileSize=1048576 # 1MB in bytes

#         # Creating a dummy file of 1MB size
#         dd if=/dev/urandom of="${dir}/$(date +%s%N).txt" bs=${fileSize} count=1 status=none
#         # head -c "${fileSize}" /dev/urandom | base64 > "${dir}/$(date +%s%N).txt"

#         # Update the current size
#         currentSize=$((currentSize + fileSize))
#     done
# done

cd ..

echo "Folders and files created successfully."