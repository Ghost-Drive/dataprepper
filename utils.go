package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"syscall"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	unixfspb "github.com/ipfs/go-unixfs/pb"
	"github.com/ipld/go-car"
	"github.com/multiformats/go-multihash"
	mh "github.com/multiformats/go-multihash"
)

func (ndwl *nodeWithLinks) constructPbNode() (pbn *merkledag.ProtoNode, err error) {
	ndb, err := ndwl.node.GetBytes()
	if err != nil {
		return
	}

	pbn = merkledag.NodeWithData(ndb)
	err = pbn.SetCidBuilder(cid.V1Builder{
		Codec:  cid.DagProtobuf,
		MhType: mh.SHA2_256,
	})
	if err != nil {
		return
	}

	for _, l := range ndwl.links {
		err = pbn.AddRawLink("", &l)
		if err != nil {
			return
		}
	}

	return
}

func (ndwl *nodeWithLinks) concatFileNode(node ipld.Node) error {

	switch node := node.(type) {

	case *merkledag.RawNode:
		s := len(node.RawData())

		ndwl.links = append(ndwl.links, ipld.Link{Cid: node.Cid()})
		ndwl.node.AddBlockSize(uint64(s))

	case *merkledag.ProtoNode:
		un, err := unixfs.ExtractFSNode(node)
		if err != nil {
			return err
		}

		switch t := un.Type(); t {
		case unixfs.TRaw, unixfs.TFile:
		default:
			return errors.New(fmt.Sprintf("can only concat raw or file types, instead found %s", t))
		}

		s := un.FileSize()

		ndwl.links = append(ndwl.links, ipld.Link{Cid: node.Cid()})
		ndwl.node.AddBlockSize(s)

	default:
		return errors.New("unknown node type")
	}

	return nil
}

func unpackCar(file string, allCids []cid.Cid) error {
	carFile, err := os.Open(file)
	if err != nil {
		fmt.Println("Error opening .car file:", err)
		return err
	}
	defer carFile.Close()

	// Create a CAR reader
	carReader, err := car.NewCarReader(carFile)
	if err != nil {
		fmt.Println("Error reading .car file:", err)
		return err
	}

	if err != nil && err != io.EOF {
		fmt.Println("Error iterating over .car file blocks:", err)
		return err
	}
	fmt.Println("Comparing CIDs")

	_comp := map[string]interface{}{}

	_carCids := []string{}
	// Check the root of the car file
	fmt.Println(carReader.Header.Roots)

	for {
		block, err := carReader.Next()
		if err != nil {
			break
		}
		_carCids = append(_carCids, block.Cid().String())
	}
	for _, _c := range allCids {
		_comp[_c.String()] = "ph"
		for _, _cc := range _carCids {
			if _cc == _c.String() {
				_comp[_c.String()] = _cc
			}
		}
	}

	// find empty missing blocks
	for _k, _v := range _comp {
		if _v == "ph" {
			fmt.Println("Missing block in car file", _k)
		}
	}

	return nil
}

func _bytesToIpldNode(fileBytes *bytes.Reader) (*merkledag.ProtoNode, error) {
	allBytes, err := io.ReadAll(fileBytes)
	if err != nil {
		return nil, err
	}

	fileNode := unixfs.NewFSNode(unixfspb.Data_File)
	fileNode.SetData(allBytes)

	allBytes = nil
	runtime.GC()

	fileNodeBytes, err := fileNode.GetBytes()
	if err != nil {
		return nil, err
	}
	defer func() { fileNodeBytes = nil }()

	fileProtoNode := merkledag.NodeWithData(fileNodeBytes)
	cidBuilder := cid.V1Builder{Codec: cid.Raw, MhType: multihash.SHA2_256}
	err = fileProtoNode.SetCidBuilder(cidBuilder)
	if err != nil {
		return nil, err
	}
	defer func() { fileProtoNode = nil }()

	debug.FreeOSMemory()
	return fileProtoNode, nil
}

func DiskCheck(folderPath *string) {
	if folderPath == nil {
		dir, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		folderPath = &dir
	}
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(*folderPath, &fs)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Calculate total and free space in bytes
	totalSpace := fs.Blocks * uint64(fs.Bsize)
	freeSpace := fs.Bfree * uint64(fs.Bsize)

	// Convert bytes to gigabytes
	totalSpaceGB := float64(totalSpace) / 1024 / 1024 / 1024
	freeSpaceGB := float64(freeSpace) / 1024 / 1024 / 1024

	fmt.Printf("Total space: %d bytes (%.2f GB)\n", totalSpace, totalSpaceGB)
	fmt.Printf("Free space: %d bytes (%.2f GB)\n", freeSpace, freeSpaceGB)
}

// FSCheck checks filesystem space details based on input and output paths.
func FSCheck(inputFolder string, outputFile *string, datastore *string) error {
	// Helper function to get filesystem statistics
	getFSStats := func(path string) (totalSpace, freeSpace uint64, err error) {
		var fs syscall.Statfs_t
		err = syscall.Statfs(path, &fs)
		if err != nil {
			return 0, 0, err
		}
		totalSpace = fs.Blocks * uint64(fs.Bsize)
		freeSpace = fs.Bfree * uint64(fs.Bsize)
		return totalSpace, freeSpace, nil
	}

	// Helper function to convert bytes to gigabytes
	bytesToGB := func(bytes uint64) float64 {
		return float64(bytes) / 1024 / 1024 / 1024
	}

	// Check total size of the input folder
	inputFolderSize, err := GetFolderSize(inputFolder)
	if err != nil {
		return fmt.Errorf("error getting size of input folder: %v", err)
	}

	// Check filesystem of inputFolder
	inputTotal, inputFree, err := getFSStats(inputFolder)
	if err != nil {
		return fmt.Errorf("error getting filesystem stats for input folder: %v", err)
	}

	// Check filesystem of outputFile
	var outputTotal, outputFree uint64
	if outputFile != nil && *outputFile != "" && !sameDrive(inputFolder, *outputFile) {
		outputTotal, outputFree, err = getFSStats(*outputFile)
		if err != nil {
			return fmt.Errorf("error getting filesystem stats for output file: %v", err)
		}
	} else {
		outputTotal, outputFree = inputTotal, inputFree
	}

	// Check filesystem of datastore
	var datastoreTotal, datastoreFree uint64
	if datastore != nil && *datastore != "" && !sameDrive(inputFolder, *datastore) && (outputFile == nil || !sameDrive(*outputFile, *datastore)) {
		datastoreTotal, datastoreFree, err = getFSStats(*datastore)
		if err != nil {
			return fmt.Errorf("error getting filesystem stats for datastore: %v", err)
		}
	} else {
		datastoreTotal, datastoreFree = inputTotal, inputFree
	}

	// Calculate total space needed after outputfile and datastore are created
	totalNeeded := inputFolderSize * 2 // Assuming outputfile and datastore will have roughly the same size as inputfolder
	remainingOutputSpace := outputFree - inputFolderSize
	remainingDatastoreSpace := datastoreFree - inputFolderSize

	// Check if there's enough free space
	if remainingOutputSpace < 0 || remainingDatastoreSpace < 0 {
		return fmt.Errorf("not enough space to create outputfile and/or datastore")
	}

	// Log the results
	fmt.Println("========================================================")
	fmt.Printf("Input Folder Size: %d (%.2f GB) Total Space: %d bytes (%.2f GB), Free Space: %d bytes (%.2f GB)\n", inputFolderSize, bytesToGB(inputFolderSize), inputTotal, bytesToGB(inputTotal), inputFree, bytesToGB(inputFree))

	if outputFile != nil && *outputFile != "" && !sameDrive(inputFolder, *outputFile) {
		fmt.Printf("Output File Drive - Total Space: %d bytes (%.2f GB), Free Space: %d bytes (%.2f GB)\n", outputTotal, bytesToGB(outputTotal), outputFree, bytesToGB(outputFree))
	}

	if datastore != nil && *datastore != "" && !sameDrive(inputFolder, *datastore) && (outputFile == nil || !sameDrive(*outputFile, *datastore)) {
		fmt.Printf("Datastore Drive - Total Space: %d bytes (%.2f GB), Free Space: %d bytes (%.2f GB)\n", datastoreTotal, bytesToGB(datastoreTotal), datastoreFree, bytesToGB(datastoreFree))
	}

	fmt.Printf("Total Space Needed: %d bytes (%.2f GB)\n", totalNeeded, bytesToGB(totalNeeded))
	fmt.Printf("Remaining Space after Creation on Output Drive: %d bytes (%.2f GB)\n", remainingOutputSpace, bytesToGB(remainingOutputSpace))
	fmt.Printf("Remaining Space after Creation on Datastore Drive: %d bytes (%.2f GB)\n", remainingDatastoreSpace, bytesToGB(remainingDatastoreSpace))
	fmt.Println("========================================================")
	return nil
}

// getFolderSize calculates the total size of a folder.
func GetFolderSize(path string) (uint64, error) {
	var size uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return nil
	})
	return size, err
}

// sameDrive checks if two paths are on the same drive.
func sameDrive(path1, path2 string) bool {
	return filepath.VolumeName(path1) == filepath.VolumeName(path2)
}

// func main() {
// 	// Example usage of FSCheck
// 	outputFile := "/path/to/output"
// 	datastore := "/path/to/datastore"
// 	err := FSCheck("/path/to/input", &outputFile, &datastore)
// 	if err != nil {
// 		fmt.Println("Error:", err)
// 	}
// }
