package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	unixfspb "github.com/ipfs/go-unixfs/pb"
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

// getFolderSize calculates the total size of a folder.
func GetFolderSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
