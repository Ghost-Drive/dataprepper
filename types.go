package main

import (
	"io/fs"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfs"
)

type NodeWithName struct {
	node ipld.Node
	name string
}

type ParentDagBuilder struct {
	maxLinks int
}

type nodeWithLinks struct {
	node  *unixfs.FSNode
	links []ipld.Link
}

type Args struct {
	OutputFileName  string
	InputFolder     string
	BadgerDatastore string
	SettingsFile    string
	ChunkSize       string
	InterimNodeSize string
}

type Dataprepper struct {
	NodesWithName        []NodeWithName
	Cids                 []cid.Cid
	Root                 []fs.DirEntry
	FileChunkSize        int64
	ProtoNodesBreakPoint int64
	Progress             struct {
		TotalSize     int64
		ProcessedSize int64
		CurrentFile   string
	}
	DagService ipld.DAGService
	UnixfsCat  ParentDagBuilder
	ParentNode struct {
		Cid   string `json:"cid"`
		Nodes []Node `json:"nodes"`
		Path  string `json:"path"`
	}
	CurrentNode Node
}

type Node struct {
	Path  string   `json:"path"`
	Cids  []string `json:"cid"`
	Nodes []Node   `json:"nodes,omitempty"`
}
