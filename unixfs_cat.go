package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
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

	// Iterate over the blocks in the CAR file
	// for {
	// 	block, err := carReader.Next()
	// 	if err != nil {
	// 		break
	// 	}

	// Process the block (for example, print the CID)
	// fmt.Println("Found CID:", block.Cid())
	// You can also access block.RawData() to get the raw bytes
	// }

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
		// fmt.Println(block.Cid().String(), _c.String())
		// if block.Cid().String() == _c.String() {
		// 	fmt.Println("Equals")
		// 	_comp[_c.String()] = block.Cid()
		// }
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

type Args struct {
	OutputFileName string
	InputFolder    string
}

type ResultSet struct {
	Cids            []cid.Cid
	Nodes           []*merkledag.ProtoNode
	UnderlyingFiles []ipld.Node
	// ParentNode      ParentNode
}

type ParentNode struct {
	Cid   string   `json:"cid"`
	Nodes []Logger `json:"nodes"`
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
	var _parentNode ParentNode
	var nodesWIthName []NodeWithName

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
			nodesWIthName = append(nodesWIthName, NodeWithName{
				node: result[0],
				name: folder.Name(),
			})
		}

		_parentNode.Nodes = append(_parentNode.Nodes, Logger{
			FolderPath: folderPath,
			Cid:        result[0].Cid().String(),
			Chunks:     chunks,
		})

		// log.Println("Concatenated result for folder:", folder.Name(), result[0])
	}

	pdb := ParentDagBuilder{maxLinks: helpers.DefaultLinksPerBlock}
	parentNode, err := pdb.ConstructParentDirectory(nodesWIthName...)
	if err != nil {
		log.Fatal(err)
	}

	_parentNode.Cid = parentNode.Cid().String()

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

		// Add parentNode to blockstore
		parentBlock, err := blocks.NewBlockWithCid(parentNode.RawData(), parentNode.Cid())
		if err != nil {
			log.Fatal(err)
		}
		err = blockstore.Put(context.Background(), parentBlock)
		if err != nil {
			log.Fatal(err)
		}

		// Write parentNode's CID to resultSet.Cids
		// resultSet.Cids = append(resultSet.Cids, parentNode.Cid())

		err = car.WriteCar(context.Background(), dagService, []cid.Cid{parentNode.Cid()}, carFile) //, resultSet.Cids, carFile)
		if err != nil {
			log.Println("Error: ", err.Error())
			os.Remove(args.OutputFileName)
		} else {
			log.Printf("Car file %v has been written.", args.OutputFileName)
			_res, err := json.MarshalIndent(_parentNode, "", "    ")
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
}
