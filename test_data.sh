#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: $0 <folder_name> <case_number>"
    exit 1
fi

folderName=$1
caseNumber=$2

mkdir -p "$folderName"
cd "$folderName"

totalSize=0
targetSize=$((6 * 1024 * 1024 * 1024)) # 6GB in bytes
fileSize=$((1900 * 1024)) # 1.9MB in bytes

case $caseNumber in
    1)
        folderCount=30
        totalFiles=$((targetSize / fileSize))

        # Create folders
        for ((i=1; i<=folderCount; i++)); do
            mkdir -p "folder_$i"
        done

        # Distribute files randomly
        while [ $totalSize -lt $targetSize ]; do
            randomFolder="folder_$((RANDOM % folderCount + 1))"
            randomSuffix=$(printf "%05d" $((RANDOM % 100000)))
            filename="0000${randomSuffix}"
            dd if=/dev/urandom of="${randomFolder}/${filename}" bs=${fileSize} count=1 status=none
            totalSize=$((totalSize + fileSize))
        done

        echo "Total size created: $totalSize bytes"

        # Print file distribution
        echo "File distribution:"
        for ((i=1; i<=folderCount; i++)); do
            folderName="folder_$i"
            fileCount=$(ls -1 "$folderName" | wc -l)
            echo "$folderName: $fileCount files"
        done
        ;;
    2)
        folderCount=$((targetSize / fileSize))

        # Create folders and files
        for ((i=1; i<=folderCount; i++)); do
            mkdir -p "folder_$i"
            dd if=/dev/urandom of="folder_$i/file" bs=${fileSize} count=1 status=none
            totalSize=$((totalSize + fileSize))
        done

        echo "Total size created: $totalSize bytes"
        echo "File distribution: $folderCount folders, each containing 1 file of size $fileSize bytes"
        ;;
    *)
        echo "Invalid case number. Please use 1 or 2."
        exit 1
        ;;
esac

cd ..

echo "Folders and files created successfully in $folderName."