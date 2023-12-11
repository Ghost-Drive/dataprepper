package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unixfs-cat/config"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	badger "github.com/ipfs/go-ds-badger2"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipld/go-car"
)

func main() {
	var args Args

	currentTime := time.Now().Unix()
	flag.StringVar(&args.OutputFileName, "o", fmt.Sprintf("ghostdrive_%v.car", currentTime), "Output filename")
	flag.StringVar(&args.InputFolder, "f", "", "Input folder")
	flag.StringVar(&args.BadgerDatastore, "d", "", "Datastore folder")
	flag.StringVar(&args.ChunkSize, "c", "", "Chunk size")
	flag.StringVar(&args.InterimNodeSize, "i", "", "Interim node size")
	// flag.StringVar(&args.SettingsFile, "s", "", "Settings file")
	flag.Parse()

	if args.InputFolder == "" {
		panic("-f flag with the following input folder is required.")
	}
	if args.BadgerDatastore == "" {
		fmt.Println("Badger datastore folder not set. Continue using in memory datastore?")

		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Continue using in-memory datastore? (Y/n): ")
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		if strings.ToLower(text) == "n" {
			log.Fatal("Exiting due to user.")
		}
	}

	// if err := FSCheck(args.InputFolder, &args.OutputFileName, &args.BadgerDatastore); err != nil {
	// 	log.Fatal(err)
	// }

	var dp Dataprepper
	var _blockstore blockstore.Blockstore

	dp.UnixfsCat = ParentDagBuilder{maxLinks: helpers.DefaultLinksPerBlock}
	dp.SetRoot(args.InputFolder)
	dp.FileChunkSize = 1 << 20
	if args.ChunkSize != "" {
		args.ChunkSize = strings.ToLower(args.ChunkSize)
		var multiplier int64 = 1
		if strings.HasSuffix(args.ChunkSize, "k") {
			multiplier = 1024
			args.ChunkSize = strings.TrimSuffix(args.ChunkSize, "k")
		} else if strings.HasSuffix(args.ChunkSize, "m") {
			multiplier = 1024 * 1024
			args.ChunkSize = strings.TrimSuffix(args.ChunkSize, "m")
		}

		chunkSize, err := strconv.ParseInt(args.ChunkSize, 10, 64)
		chunkSize *= multiplier

		if err != nil {
			log.Fatal("Failed to convert Chunk Size to int64:", err)
		}
		dp.FileChunkSize = chunkSize
	}

	dp.ProtoNodesBreakPoint = 10 << 20
	if args.InterimNodeSize != "" {
		args.InterimNodeSize = strings.ToLower(args.InterimNodeSize)
		var multiplier int64 = 1
		if strings.HasSuffix(args.InterimNodeSize, "k") {
			multiplier = 1024
			args.InterimNodeSize = strings.TrimSuffix(args.InterimNodeSize, "k")
		} else if strings.HasSuffix(args.InterimNodeSize, "m") {
			multiplier = 1024 * 1024
			args.InterimNodeSize = strings.TrimSuffix(args.InterimNodeSize, "m")
		}

		interimNodeSize, err := strconv.ParseInt(args.InterimNodeSize, 10, 64)
		interimNodeSize *= multiplier

		if err != nil {
			log.Fatal("Failed to convert Interim Node Size to int64:", err)
		}

		dp.ProtoNodesBreakPoint = interimNodeSize
	}

	totalSize, err := GetFolderSize(args.InputFolder)
	if err != nil {
		log.Fatal(err)
	}

	dp.Progress.TotalSize = int64(totalSize)
	dp.Progress.ProcessedSize = 0

	carFile, err := os.Create(args.OutputFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer carFile.Close()

	// folder for datastore
	if args.BadgerDatastore != "" {
		os.MkdirAll(args.BadgerDatastore, os.ModePerm)

		opts := badger.DefaultOptions

		// opts
		opts.SyncWrites = config.Settings.BadgerOptions.SyncWrites
		opts.ValueLogFileSize = config.Settings.BadgerOptions.ValueLogFileSize

		if ds, err := badger.NewDatastore(args.BadgerDatastore, &opts); err == nil {
			_blockstore = blockstore.NewBlockstore(ds)
		} else {
			panic(err)
		}
	} else {
		ds := sync.MutexWrap(datastore.NewMapDatastore())
		_blockstore = blockstore.NewBlockstore(ds)
	}

	exchange := offline.Exchange(_blockstore)
	blockService := blockservice.New(_blockstore, exchange)
	dp.DagService = merkledag.NewDAGService(blockService)

	fmt.Println("Starting file processing.")
	if err := dp.TraverseAndCreateNodes(args.InputFolder); err != nil {
		log.Fatal(err)
	}

	// pdb := ParentDagBuilder{maxLinks: helpers.DefaultLinksPerBlock}
	parentNode, err := dp.UnixfsCat.ConstructParentDirectory(dp.NodesWithName...)
	if err != nil {
		log.Fatal(err)
	}

	dp.AddDag(parentNode)

	fmt.Printf("\nWriting .car file %v, CID: %v. Please wait...\n", args.OutputFileName, parentNode.Cid().String())

	dp.ParentNode.Cid = parentNode.Cid().String()
	dp.ParentNode.Nodes = dp.CurrentNode.Nodes
	dp.ParentNode.Path = args.InputFolder

	err = car.WriteCar(context.Background(), dp.DagService, []cid.Cid{parentNode.Cid()}, carFile) //, resultSet.Cids, carFile)
	if err != nil {
		log.Println("Error: ", err.Error())
		os.Remove(args.OutputFileName)
	} else {
		log.Printf("Car file %v has been written. Blocks: %v", args.OutputFileName, len(dp.Cids))

		if args.BadgerDatastore != "" {
			if err := os.RemoveAll(args.BadgerDatastore); err != nil {
				log.Println("Failed to remove Badger datastore directory:", err)
			}
		}

		_res, err := json.MarshalIndent(dp.ParentNode, "", "    ")
		if err != nil {
			log.Fatal("Error:", err)
		}

		jsonFileName := strings.TrimSuffix(args.OutputFileName, filepath.Ext(args.OutputFileName)) + ".json"
		jsonFile, err := os.Create(jsonFileName)

		if err != nil {
			log.Fatal("Error:", err)
		}
		defer jsonFile.Close()

		_, err = jsonFile.Write(_res)
		if err != nil {
			log.Fatal("Error:", err)
		}
		log.Printf("Json file %v has been written.", jsonFileName)
	}
}
