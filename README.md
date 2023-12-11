App to concat files in chunks and create .car file from them.

to build: go build -o [app_name]

to run: ./[app_name] -f [input_folder] -o [output_filename] -d [datastore_folder]

or use file dataprepper -f [input_folder] -o [output_filename] -d [datastore_folder]

to set chunk size and interim node size (optional): -c [chunk_size]k/m -i [interim_node_size]k/m

to generate test structure: chmod +x test_data.sh && ./test_data.sh

TODO:
-- Fix returned JSON file with DAG structure
-- -c ChunkSize and -i InterimBlockSize -- implemented, missing checks
-- Goroutines where splitting file to chunks
-- Tests