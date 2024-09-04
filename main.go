package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
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
	// debug.SetMemoryLimit(4 * 1024 * 1024 * 1024) // 4 GB
	// debug.SetGCPercent(50)

	var args Args

	currentTime := time.Now().Unix()
	flag.StringVar(&args.OutputFileName, "o", fmt.Sprintf("ghostdrive_%v.car", currentTime), "Output filename")
	flag.StringVar(&args.InputFolder, "f", "", "Input folder")
	flag.StringVar(&args.BadgerDatastore, "d", "", "Datastore folder")
	flag.StringVar(&args.ChunkSize, "c", "", "Chunk size")
	flag.StringVar(&args.InterimNodeSize, "i", "", "Interim node size")
	flag.IntVar(&args.MaxLinksPerBlock, "m", 0, "Max Links per Block")
	args.Silent = flag.Bool("silent", false, "Silence all output")
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

	if *args.Silent {
		log.SetOutput(io.Discard)
		originalStdout := os.Stdout
		temp, _ := os.CreateTemp("", "temp-stdout")
		os.Stdout = temp

		defer func() {
			os.Stdout = originalStdout
			temp.Close()
			os.Remove(temp.Name())
		}()

		devNull, _ := os.Open(os.DevNull)
		syscall.Dup2(int(devNull.Fd()), int(os.Stdout.Fd()))
	}

	var dp Dataprepper
	var _blockstore blockstore.Blockstore

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

	// fmt.Println("count of possible links per block", dp.ProtoNodesBreakPoint/dp.FileChunkSize)
	_possibleLinksPerBlock := dp.ProtoNodesBreakPoint / dp.FileChunkSize
	if (_possibleLinksPerBlock < int64(helpers.DefaultLinksPerBlock)) || (args.MaxLinksPerBlock != 0 && args.MaxLinksPerBlock < helpers.BlockSizeLimit && args.MaxLinksPerBlock > int(_possibleLinksPerBlock)) {
		args.MaxLinksPerBlock = helpers.DefaultLinksPerBlock
	} else {
		args.MaxLinksPerBlock = int(_possibleLinksPerBlock) + 1
	}
	// fmt.Println("Max Links Per Block", args.MaxLinksPerBlock)

	dp.UnixfsCat = ParentDagBuilder{maxLinks: args.MaxLinksPerBlock}
	// if args.MaxLinksPerBlock == 0 {
	// 	if _possibleLinksPerBlock < int64(helpers.DefaultLinksPerBlock) {
	// 		args.MaxLinksPerBlock = helpers.DefaultLinksPerBlock
	// 	} else {
	// 		args.MaxLinksPerBlock = int(_possibleLinksPerBlock) + 1
	// 	}
	// } else {
	// 	if args.MaxLinksPerBlock < helpers.BlockSizeLimit {
	// 		args.MaxLinksPerBlock = helpers.DefaultLinksPerBlock
	// 	} else {
	// 		args.MaxLinksPerBlock = int(_possibleLinksPerBlock) + 1
	// 	}
	// }

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
	dp.ParentNode.Path = args.InputFolder

	err = car.WriteCar(context.Background(), dp.DagService, []cid.Cid{parentNode.Cid()}, carFile) //, resultSet.Cids, carFile)
	if err != nil {
		log.Println("Error: ", err.Error())
		os.Remove(args.OutputFileName)
	} else {
		dp.Cids = RemoveDuplicates(dp.Cids)
		log.Printf("Car file %v has been written. Blocks: %v", args.OutputFileName, len(dp.Cids))

		if args.BadgerDatastore != "" {
			if err := os.RemoveAll(args.BadgerDatastore); err != nil {
				log.Println("Failed to remove Badger datastore directory:", err)
			}
		}

		jsonFileName := strings.TrimSuffix(args.OutputFileName, filepath.Ext(args.OutputFileName)) + ".jsonl"
		jsonFile, err := os.Create(jsonFileName)
		if err != nil {
			log.Fatal("Error:", err)
		}
		defer jsonFile.Close()

		jsonFile.WriteString(fmt.Sprintf("{\"root\": \"%s\"}\n", dp.ParentNode.Cid))
		encoder := json.NewEncoder(jsonFile)
		// encoder.SetIndent("", "  ")
		for _, node := range dp.ParentNode.Nodes {
			if err := encoder.Encode(node); err != nil {
				log.Fatalf("Error writing node to jsonl file: %v", err)
			}
		}

		log.Printf("Json file %v has been written.", jsonFileName)
	}
}
