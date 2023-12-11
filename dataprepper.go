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

func (dp *Dataprepper) TraverseAndCreateNodes(dir string) error {
	// var _logger_currentNodes, _logger_currentParentNodes, _logger_parentNodes []Node
	var _logger_file_chunks_interims, _logger_file_chunks_chunks, _logger_named []Node
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

		var _chunkedProtoNodes, interimProtoNodes []ipld.Node
		// var _chunkedProtoNodes []ipld.Node
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

			// Logger
			dp.CurrentNode = Node{
				Path: dp.Progress.CurrentFile,
			}

			_protoNodes, err := dp.FileToProtoNode(dp.Progress.CurrentFile)
			if err != nil {
				log.Fatal(err)
			}

			for _, _pn := range _protoNodes {
				_chunkedProtoNodes = append(_chunkedProtoNodes, _pn)

				// Logger
				dp.CurrentNode.Cids = append(dp.CurrentNode.Cids, _pn.Cid().String())
			}

			_logger_file_chunks_chunks = append(_logger_file_chunks_chunks, dp.CurrentNode)

			// fmt.Println("INTERIM ", dp.CurrentNode.Nodes)
			// set currentNode
			// _currentNodes = append(_currentNodes, dp.CurrentNode)

			_protoNodes = nil

			_currentSize += info.Size()

			if _currentSize >= dp.ProtoNodesBreakPoint {
				_concatedChunkedProtoNodes, err := dp.UnixfsCat.ConcatFileNodes(_chunkedProtoNodes...)
				if err != nil {
					log.Fatal(err)
				}
				_logger_file_chunks_interim := Node{
					Path: fmt.Sprintf("interim/%v", folderPath),
				}

				// _currentParentNode := Node{
				// 	path: dp.progress.currentfile,
				// }

				for _, _ccpn := range _concatedChunkedProtoNodes {
					interimProtoNodes = append(interimProtoNodes, _ccpn)

					_logger_file_chunks_interim.Cids = append(_logger_file_chunks_interim.Cids, _ccpn.Cid().String())
					// _currentParentNode.Cids = append(_currentParentNode.Cids, _ccpn.Cid().String())

					dp.AddDag(_ccpn)
				}

				_chunkedProtoNodes = []ipld.Node{}
				_currentSize = 0

				_logger_file_chunks_interim.Nodes = _logger_file_chunks_chunks
				_logger_file_chunks_interims = append(_logger_file_chunks_interims, _logger_file_chunks_interim)
				_logger_file_chunks_chunks = []Node{}
				// _currentNodes = []Node{}
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

			// _currentParentNode := Node{
			// 	Path: dp.Progress.CurrentFile,
			// }
			_logger_file_chunks_interim := Node{
				Path: fmt.Sprintf("interim/%v", folderPath),
			}

			_logger_folder := Node{
				Path: folderPath,
			}

			// if len(interimProtoNodes) > 0 {

			// }

			for _, _ccpn := range _concatedChunkedProtoNodes {
				dp.AddDag(_ccpn)

				if len(interimProtoNodes) > 0 {
					interimProtoNodes = append(interimProtoNodes, _ccpn)

					_logger_file_chunks_interim.Cids = append(_logger_file_chunks_interim.Cids, _ccpn.Cid().String())

					// _currentParentNode.Cids = append(_currentParentNode.Cids, _ccpn.Cid().String())
				} else {
					dp.SetNodesWithName(_ccpn, d.Name())

					for _, _nwn := range dp.NodesWithName {
						_logger_folder.Cids = append(_logger_folder.Cids, _nwn.node.Cid().String())
					}
				}
			}

			if len(interimProtoNodes) > 0 {
				_logger_file_chunks_interim.Nodes = _logger_file_chunks_chunks
				_logger_file_chunks_interims = append(_logger_file_chunks_interims, _logger_file_chunks_interim)
				_logger_file_chunks_chunks = []Node{}

				// _currentParentNode.Nodes = _currentNodes
				// _currentParentNodes = append(_currentParentNodes, _currentParentNode)
				// _currentNodes = []Node{}
			} else {
				fmt.Print(_logger_file_chunks_chunks)
				_logger_folder.Nodes = _logger_file_chunks_chunks
				_logger_named = append(_logger_named, _logger_folder)
			}
		}

		if len(interimProtoNodes) > 0 {
			_concatedFIleNodes, err := dp.UnixfsCat.ConcatFileNodes(interimProtoNodes...)
			if err != nil {
				log.Fatal(err)
			}

			_logger_folder := Node{
				Path: folderPath,
			}

			// _parentNode := Node{
			// 	Path: folderPath,
			// }

			for _, _cfn := range _concatedFIleNodes {
				dp.SetNodesWithName(_cfn, d.Name())

				for _, _nwn := range dp.NodesWithName {
					_logger_folder.Cids = append(_logger_folder.Cids, _nwn.node.Cid().String())
				}
				// _parentNode.Cids = append(_parentNode.Cids, _cfn.Cid().String())

				dp.AddDag(_cfn)

				_logger_folder.Nodes = _logger_file_chunks_interims
				_logger_named = append(_logger_named, _logger_folder)

			}
			_concatedFIleNodes = nil
			// _parentNode.Nodes = _currentParentNodes
			// _currentParentNodes = []Node{}
		}

		runtime.GC()
	}

	dp.CurrentNode = Node{
		Nodes: _logger_named,
	}

	return nil
}

func (dp *Dataprepper) _fileToProtoNode(file *os.File) ([]*merkledag.ProtoNode, error) {
	var nodes []ipld.Node

	reader := bufio.NewReader(file)

	_currentBytes := 0

	var protoNodes []*merkledag.ProtoNode
	var _logger_chunks, _logger_interims []Node

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

		_logger_chunk := Node{
			Path: fmt.Sprintf("%v/chunk_%v", dp.CurrentNode.Path, _i),
		}

		_logger_chunk.Cids = append(_logger_chunk.Cids, node.Cid().String())
		_logger_chunks = append(_logger_chunks, _logger_chunk)

		// dp.CurrentNode.Nodes = append(dp.CurrentNode.Nodes, _nodeLogger)

		_currentBytes += n

		if int64(_currentBytes) >= dp.ProtoNodesBreakPoint {

			_chunkedProtoNodes, err := dp.UnixfsCat.ConcatFileNodes(nodes...)
			if err != nil {
				log.Fatal(err)
			}

			_logger_interim := Node{
				Path: fmt.Sprintf("%v/interim_%v", dp.CurrentNode.Path, _i),
			}

			for _, _cpn := range _chunkedProtoNodes {
				dp.AddDag(_cpn)
				protoNodes = append(protoNodes, _cpn)

				_logger_interim.Cids = append(_logger_interim.Cids, _cpn.Cid().String())
			}

			_logger_interim.Nodes = _logger_chunks
			_logger_chunks = []Node{}
			_logger_interims = append(_logger_interims, _logger_interim)

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
		_logger_interim := Node{
			Path: fmt.Sprintf("%v/interim_%v", dp.CurrentNode.Path, _i),
		}

		for _, _cpn := range _chunkedProtoNodes {
			dp.AddDag(_cpn)
			protoNodes = append(protoNodes, _cpn)

			_logger_interim.Cids = append(_logger_interim.Cids, _cpn.Cid().String())
		}

		_logger_interim.Nodes = _logger_chunks
		_logger_interims = append(_logger_interims, _logger_interim)
	}

	dp.CurrentNode.Nodes = _logger_interims

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
