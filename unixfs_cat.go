package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/importer/helpers"
	unixfspb "github.com/ipfs/go-unixfs/pb"
	"github.com/ipld/go-car"
	"github.com/multiformats/go-multihash"
	mh "github.com/multiformats/go-multihash"
)

type nodeWithLinks struct {
	node  *unixfs.FSNode
	links []ipld.Link
}

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

type NodeWithName struct {
	node ipld.Node
	name string
}

type ParentDagBuilder struct {
	maxLinks int
}

func (pdb ParentDagBuilder) ConcatFileNodes(nodes ...ipld.Node) ([]*merkledag.ProtoNode, error) {
	var pbns []*merkledag.ProtoNode
	ndwl := nodeWithLinks{node: unixfs.NewFSNode(unixfspb.Data_File)}
	for _, node := range nodes {
		if len(ndwl.node.BlockSizes()) < pdb.maxLinks {
			if err := ndwl.concatFileNode(node); err != nil {
				return nil, err
			}
		} else {
			pbn, err := ndwl.constructPbNode()
			if err != nil {
				return nil, err
			}

			pbns = append(pbns, pbn)

			ndwl = nodeWithLinks{node: unixfs.NewFSNode(unixfspb.Data_File)}
			if err := ndwl.concatFileNode(node); err != nil {
				return nil, err
			}

		}
	}

	pbn, err := ndwl.constructPbNode()
	if err != nil {
		return nil, err
	}

	pbns = append(pbns, pbn)

	return pbns, nil
}

func (pdb ParentDagBuilder) ConstructParentDirectory(nodes ...NodeWithName) (*merkledag.ProtoNode, error) {
	ndbs, err := unixfs.NewFSNode(unixfspb.Data_Directory).GetBytes()
	if err != nil {
		return nil, err
	}
	nd := merkledag.NodeWithData(ndbs)
	err = nd.SetCidBuilder(cid.V1Builder{Codec: cid.DagProtobuf, MhType: multihash.SHA2_256})
	if err != nil {
		return nil, err
	}

	for _, node := range nodes {
		s, _ := node.node.Size()
		err = nd.AddRawLink(node.name, &ipld.Link{Cid: node.node.Cid(), Size: s})
		if err != nil {
			return nil, err
		}
	}

	return nd, nil
}

var fileToIPLDNode = func(file os.FileInfo, path string) (*merkledag.ProtoNode, error) {
	fileData, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fileNode := unixfs.NewFSNode(unixfspb.Data_File)
	fileNode.SetData(fileData)
	fileNodeBytes, err := fileNode.GetBytes()
	if err != nil {
		return nil, err
	}
	fileProtoNode := merkledag.NodeWithData(fileNodeBytes)
	cidBuilder := cid.V1Builder{Codec: cid.Raw, MhType: multihash.SHA2_256}
	err = fileProtoNode.SetCidBuilder(cidBuilder)
	if err != nil {
		return nil, err
	}
	return fileProtoNode, nil
}

// func unpackCar() error {
// 	log.Println("Reading car file")
// 	carFile, err := os.Open("output.car")
// 	if err != nil {
// 		return fmt.Errorf("failed to open car file: %w", err)
// 	}
// 	defer carFile.Close()

// 	// Create a Blockstore to store the blocks
// 	// bs := blockstore.NewBlockstore(datastore.NewMapDatastore())

// 	// Create a CAR file reader
// 	carReader, err := car.NewCarReader(carFile)
// 	if err != nil {
// 		return fmt.Errorf("failed to create car reader: %w", err)
// 	}

// 	// Iterate through the blocks in the CAR file

// 	for _, _r := range carReader.Header.Roots {
// 		log.Println("Car roots:", _r)
// 	}

// 	return nil
// }

type Args struct {
	OutputFileName string
	InputFolder    string
}

type ResultSet struct {
	Cids            []cid.Cid
	Nodes           []*merkledag.ProtoNode
	UnderlyingFiles []ipld.Node
	Logger          []Logger
}
type Logger struct {
	FolderPath string  `json:"folder_path"`
	Cid        string  `json:"cid"`
	Chunks     []Chunk `json:"chunks"`
}

type Chunk struct {
	FilePath string `json:"file_path"`
	Cid      string `json:"cid"`
}

func main() {
	var args Args

	currentTime := time.Now().Unix()
	flag.StringVar(&args.OutputFileName, "o", fmt.Sprintf("ghostdrive_%v.car", currentTime), "Output filename")
	flag.StringVar(&args.InputFolder, "f", "", "Input folder")
	flag.Parse()
	if args.InputFolder == "" {
		panic("-f flag with the following input folder is required.")
	}

	folders, err := ioutil.ReadDir(args.InputFolder)
	if err != nil {
		log.Fatal(err)
	}

	var resultSet ResultSet

	for _, folder := range folders {
		if !folder.IsDir() {
			continue
		}

		var chunks []Chunk
		folderPath := filepath.Join(args.InputFolder, folder.Name())
		files, err := ioutil.ReadDir(folderPath)
		if err != nil {
			log.Fatal(err)
		}

		var fileNodes []ipld.Node

		for _, file := range files {
			if file.IsDir() {
				continue
			}
			filePath := filepath.Join(folderPath, file.Name())
			fileProtoNode, err := fileToIPLDNode(file, filePath)
			if err != nil {
				log.Fatal(err)
			}
			// log.Println("Filepath:", filePath, "CID:", fileProtoNode.Cid())
			fileNodes = append(fileNodes, fileProtoNode)
			resultSet.UnderlyingFiles = append(resultSet.UnderlyingFiles, fileProtoNode)
			resultSet.Cids = append(resultSet.Cids, fileProtoNode.Cid())
			chunks = append(chunks, Chunk{
				Cid:      fileProtoNode.Cid().String(),
				FilePath: filePath,
			})
		}

		pdb := ParentDagBuilder{maxLinks: helpers.DefaultLinksPerBlock}

		result, err := pdb.ConcatFileNodes(fileNodes...)
		if err != nil {
			log.Fatal(err)
		}
		if len(result) > 0 {
			resultSet.Cids = append(resultSet.Cids, result[0].Cid())
			resultSet.Nodes = append(resultSet.Nodes, result[0])
		}

		resultSet.Logger = append(resultSet.Logger, Logger{
			FolderPath: folderPath,
			Cid:        result[0].Cid().String(),
			Chunks:     chunks,
		})

		// log.Println("Concatenated result for folder:", folder.Name(), result[0])
	}

	if len(resultSet.Cids) > 0 {
		carFile, err := os.Create(args.OutputFileName)
		if err != nil {
			log.Fatal(err)
		}
		defer carFile.Close()

		blockstore := blockstore.NewBlockstore(datastore.NewMapDatastore())
		exchange := offline.Exchange(blockstore)
		blockService := blockservice.New(blockstore, exchange)
		dagService := merkledag.NewDAGService(blockService)

		for _, node := range resultSet.Nodes {
			block, err := blocks.NewBlockWithCid(node.RawData(), node.Cid())
			if err != nil {
				log.Fatal(err)
			}
			err = blockstore.Put(context.Background(), block)
			if err != nil {
				log.Fatal(err)
			}
		}

		for _, _files := range resultSet.UnderlyingFiles {
			block, err := blocks.NewBlockWithCid(_files.RawData(), _files.Cid())
			if err != nil {
				log.Fatal(err)
			}
			err = blockstore.Put(context.Background(), block)
			if err != nil {
				log.Fatal(err)
			}
		}

		err = car.WriteCar(context.Background(), dagService, resultSet.Cids, carFile)
		if err != nil {
			log.Println("Error: ", err.Error())
			os.Remove(args.OutputFileName)
		} else {
			log.Printf("Car file %v has been written.", args.OutputFileName)
			_res, err := json.Marshal(resultSet.Logger)
			if err != nil {
				log.Fatal("Error:", err)
			}
			fmt.Println(string(_res))
			// unpackCar()
		}
	}
}
