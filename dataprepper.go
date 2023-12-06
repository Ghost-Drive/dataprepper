package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"

	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
)

func (dp *Dataprepper) SetRoot(dir string) {
	root, err := os.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}
	dp.Root = root
}

func (dp *Dataprepper) SetNodesWithName(protoNode *merkledag.ProtoNode, nodeName string) {
	dp.NodesWithName = append(dp.NodesWithName, NodeWithName{
		node: protoNode,
		name: nodeName,
	})
}

func (dp *Dataprepper) AddDag(protoNode *merkledag.ProtoNode) {
	if err := dp.DagService.Add(context.Background(), protoNode); err != nil {
		log.Fatal(err)
	}
	dp.Cids = append(dp.Cids, protoNode.Cid())
}

var _currentNodes, _currentParentNodes, _parentNodes []Node

func (dp *Dataprepper) TraverseAndCreateNodes(dir string) error {
	for _, d := range dp.Root {
		if !d.IsDir() {
			log.Println("Found file", d.Name(), "Skipping...")
			continue
		}

		folderPath := filepath.Join(dir, d.Name())
		entries, err := os.ReadDir(folderPath)
		if err != nil {
			log.Fatal(err)
		}

		var interimProtoNodes []ipld.Node
		var _chunkedProtoNodes []ipld.Node
		var _currentSize int64

		for _, entry := range entries {
			if entry.IsDir() {
				log.Println("Found folder", entry.Name(), "Skipping...")
				continue
			}

			info, err := entry.Info()
			if err != nil {
				log.Fatal(err)
			}

			dp.Progress.CurrentFile = filepath.Join(folderPath, entry.Name())

			dp.CurrentNode = Node{
				Path: dp.Progress.CurrentFile,
			}

			_protoNodes, err := dp.FileToProtoNode(dp.Progress.CurrentFile)
			if err != nil {
				log.Fatal(err)
			}
			for _, _pn := range _protoNodes {
				_chunkedProtoNodes = append(_chunkedProtoNodes, _pn)
				dp.CurrentNode.Cids = append(dp.CurrentNode.Cids, _pn.Cid().String())
			}
			// set currentNode
			_currentNodes = append(_currentNodes, dp.CurrentNode)

			_protoNodes = nil

			_currentSize += info.Size()

			if _currentSize >= dp.ProtoNodesBreakPoint {
				_concatedChunkedProtoNodes, err := dp.UnixfsCat.ConcatFileNodes(_chunkedProtoNodes...)
				if err != nil {
					log.Fatal(err)
				}
				_currentParentNode := Node{
					Path: dp.Progress.CurrentFile,
				}

				for _, _ccpn := range _concatedChunkedProtoNodes {
					interimProtoNodes = append(interimProtoNodes, _ccpn)

					_currentParentNode.Cids = append(_currentParentNode.Cids, _ccpn.Cid().String())

					dp.AddDag(_ccpn)
				}

				_chunkedProtoNodes = []ipld.Node{}
				_currentSize = 0

				_currentParentNode.Nodes = _currentNodes
				_currentParentNodes = append(_currentParentNodes, _currentParentNode)
				_currentNodes = []Node{}
			}
			runtime.GC()
			debug.FreeOSMemory()

			dp.DisplayProgress(true)
		}

		if len(_chunkedProtoNodes) > 0 {
			_concatedChunkedProtoNodes, err := dp.UnixfsCat.ConcatFileNodes(_chunkedProtoNodes...)
			if err != nil {
				log.Fatal(err)
			}

			_currentParentNode := Node{
				Path: dp.Progress.CurrentFile,
			}

			for _, _ccpn := range _concatedChunkedProtoNodes {
				dp.AddDag(_ccpn)

				if len(interimProtoNodes) > 0 {
					interimProtoNodes = append(interimProtoNodes, _ccpn)

					_currentParentNode.Cids = append(_currentParentNode.Cids, _ccpn.Cid().String())
				} else {
					dp.SetNodesWithName(_ccpn, d.Name())
				}
			}

			if len(interimProtoNodes) > 0 {
				_currentParentNode.Nodes = _currentNodes
				_currentParentNodes = append(_currentParentNodes, _currentParentNode)
				_currentNodes = []Node{}
			}
		}

		if len(interimProtoNodes) > 0 {
			_concatedFIleNodes, err := dp.UnixfsCat.ConcatFileNodes(interimProtoNodes...)
			if err != nil {
				log.Fatal(err)
			}

			_parentNode := Node{
				Path: folderPath,
			}

			for _, _cfn := range _concatedFIleNodes {
				dp.SetNodesWithName(_cfn, d.Name())
				_parentNode.Cids = append(_parentNode.Cids, _cfn.Cid().String())

				dp.AddDag(_cfn)

			}
			_concatedFIleNodes = nil
			_parentNode.Nodes = _currentParentNodes
			_currentParentNodes = []Node{}
		}

		runtime.GC()
	}
	dp.CurrentNode = Node{
		Nodes: _parentNodes,
	}

	return nil
}

func (dp *Dataprepper) _fileToProtoNode(file *os.File) ([]*merkledag.ProtoNode, error) {
	var nodes []ipld.Node

	reader := bufio.NewReader(file)

	_currentBytes := 0

	var protoNodes []*merkledag.ProtoNode
	var _nodesLogger []Node
	_i := 1
	for {
		chunk := make([]byte, dp.FileChunkSize)
		n, err := reader.Read(chunk)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
			// return nil, nil, err
		}

		node, err := _bytesToIpldNode(bytes.NewReader(chunk[:n]))
		if err != nil {
			log.Fatal(err)
		}

		nodes = append(nodes, node)
		dp.AddDag(node)

		_nodeLogger := Node{
			Path: fmt.Sprintf("%v/chunk_%v", dp.CurrentNode.Path, _i),
		}
		_nodeLogger.Cids = append(_nodeLogger.Cids, node.Cid().String())

		dp.CurrentNode.Nodes = append(dp.CurrentNode.Nodes, _nodeLogger)

		_currentBytes += n

		if int64(_currentBytes) >= dp.ProtoNodesBreakPoint {

			_chunkedProtoNodes, err := dp.UnixfsCat.ConcatFileNodes(nodes...)
			if err != nil {
				log.Fatal(err)
			}
			for _, _cpn := range _chunkedProtoNodes {
				dp.AddDag(_cpn)
				protoNodes = append(protoNodes, _cpn)
			}

			nodes = []ipld.Node{}
			_currentBytes = 0
		}

		currentChunkSize := dp.FileChunkSize
		if int64(n) < dp.FileChunkSize {
			currentChunkSize = int64(n)
		}
		dp.Progress.ProcessedSize += currentChunkSize
		dp.DisplayProgress(false)
		_i++
	}
	if len(nodes) > 0 {
		_chunkedProtoNodes, err := dp.UnixfsCat.ConcatFileNodes(nodes...)
		if err != nil {
			log.Fatal(err)
		}
		for _, _cpn := range _chunkedProtoNodes {
			dp.AddDag(_cpn)
			protoNodes = append(protoNodes, _cpn)
		}
	}
	dp.CurrentNode.Nodes = _nodesLogger

	return protoNodes, nil
}

func (dp *Dataprepper) FileToProtoNode(filePath string) ([]*merkledag.ProtoNode, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return dp._fileToProtoNode(file)
}

func (dp *Dataprepper) DisplayProgress(commit bool) {
	percent := (float64(dp.Progress.ProcessedSize) / float64(dp.Progress.TotalSize)) * 100

	progressStr := fmt.Sprintf("\rProcessing %v: %.2f%% ", dp.Progress.CurrentFile, percent)
	padding := 100 - len(progressStr)
	if padding > 0 {
		progressStr += strings.Repeat(" ", padding)
	}

	fmt.Print(progressStr)
	os.Stdout.Sync()
}
