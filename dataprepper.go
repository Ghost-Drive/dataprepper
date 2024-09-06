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
	"golang.org/x/term"
)

func (dp *Dataprepper) SetRoot(dir string) {
	root, err := os.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}
	dp.Root = root
}

func (dp *Dataprepper) ConcatFiles(protoNodes []*merkledag.ProtoNode, setNodeWithName *bool, nodeName *string) {

}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func (dp *Dataprepper) SetNodesWithName(protoNode *merkledag.ProtoNode, nodeName string) {
	// Append to NodesWithName
	// Append to NodesWithName
	dp.NodesWithName = append(dp.NodesWithName, NodeWithName{
		node: protoNode,
		name: nodeName,
	})

	// Dump NodesWithName to filesystem to free up memory
	// if len(dp.NodesWithName) >= 1000 { // Adjust this threshold as needed
	// 	tempFile, err := os.CreateTemp("", "nodesWithName_*.tmp")
	// 	if err != nil {
	// 		log.Printf("Error creating temp file: %v", err)
	// 	} else {
	// 		defer tempFile.Close()
	// 		encoder := json.NewEncoder(tempFile)
	// 		for _, nwn := range dp.NodesWithName {
	// 			if err := encoder.Encode(nwn); err != nil {
	// 				log.Printf("Error encoding NodeWithName: %v", err)
	// 			}
	// 		}
	// 		// Clear the slice after dumping
	// 		dp.NodesWithName = dp.NodesWithName[:0]
	// 	}
	// }
}

func (dp *Dataprepper) AddDag(protoNode *merkledag.ProtoNode) {
	if err := dp.DagService.Add(context.Background(), protoNode); err != nil {
		log.Fatal(err)
	}
	dp.Cids = append(dp.Cids, protoNode.Cid())
}

func (dp *Dataprepper) TraverseAndCreateNodes(dir string) error {
	for _, d := range dp.Root {
		if !d.IsDir() {
			// log.Println("Found file", d.Name(), "Skipping...")
			continue
		}

		folderPath := filepath.Join(dir, d.Name())
		entries, err := os.ReadDir(folderPath)
		if err != nil {
			log.Fatal(err)
		}

		// grab total folder size
		folderSize, err := GetFolderSize(folderPath)
		if err != nil {
			log.Fatal(err)
		}

		var _chunkedProtoNodes, interimProtoNodes []ipld.Node
		// var _chunkedProtoNodes []ipld.Node
		var _currentSize int64

		// check if there's a need to form interims
		_needsToSplitToInterim := folderSize > dp.ProtoNodesBreakPoint

		// logger 1 folder = 1 json
		// create _logger_folder_object
		dp.CurrentFolder = Node{
			Path: d.Name(),
		}

		if _needsToSplitToInterim {
			dp.CurrentInterim = &Node{
				Path: fmt.Sprintf("%v/interim_%v", d.Name(), len(dp.CurrentFolder.Nodes)+1),
			}
		}

		for _, entry := range entries {
			if entry.IsDir() {
				// log.Println("Found folder", entry.Name(), "Skipping...")
				continue
			}

			info, err := entry.Info()
			if err != nil {
				log.Fatal(err)
			}

			dp.Progress.CurrentFile = filepath.Join(folderPath, entry.Name())

			// Logger
			dp.CurrentNode = Node{
				Path: filepath.Base(dp.Progress.CurrentFile),
			}

			/*
				1. Convert file to ipld.Node
				2. Recieve slice of *merkledag.ProtoNode
				3. if size of files converted to ipld.Node >= protonodebreakpoint, pack them into interim, and clean them up from memory
				4. if size of files converted to ipld.Node < protonodebreakpoint after app done with folder, pack them into NamedNode
				5. All interims of a folder then packed into NamedNode
				6. If there's not enough files to form an interim, pack them into NamedNode
			*/

			_protoNodes, err := dp.FileToProtoNode(dp.Progress.CurrentFile)
			if err != nil {
				log.Fatal(err)
			}

			for _, _pn := range _protoNodes {
				_chunkedProtoNodes = append(_chunkedProtoNodes, _pn)

				// Logger
				// dp.CurrentNode.Cids = append(dp.CurrentNode.Cids, _pn.Cid().String())
			}
			dp.CurrentNode.Cid = _protoNodes[0].Cid().String()

			// cleanup
			_protoNodes = nil

			_currentSize += info.Size()

			if !_needsToSplitToInterim {
				// if there won't be any need for interims then just add nodes to current folder
				dp.CurrentFolder.Nodes = append(dp.CurrentFolder.Nodes, dp.CurrentNode)
			} else {
				// append dp.CurrentNode to currentinterim
				dp.CurrentInterim.Nodes = append(dp.CurrentInterim.Nodes, dp.CurrentNode)
			}

			// 1. If files in folder is enough to form interim, and there are more files to come
			if _currentSize >= dp.ProtoNodesBreakPoint && _needsToSplitToInterim {
				_concatedChunkedProtoNodes, err := dp.UnixfsCat.ConcatFileNodes(_chunkedProtoNodes...)
				if err != nil {
					log.Fatal(err)
				}

				for len(_concatedChunkedProtoNodes) > 1 {
					// Create a new slice with the type []format.Node
					var formatNodes []ipld.Node
					for _, protoNode := range _concatedChunkedProtoNodes {
						formatNodes = append(formatNodes, protoNode)
					}

					// Use the new slice in the ConcatFileNodes function
					_concatedChunkedProtoNodes, err = dp.UnixfsCat.ConcatFileNodes(formatNodes...)
					for _, _ccpn := range _concatedChunkedProtoNodes {
						dp.AddDag(_ccpn)
					}
					if err != nil {
						log.Fatal(err)
					}
				}

				for _, _ccpn := range _concatedChunkedProtoNodes {
					interimProtoNodes = append(interimProtoNodes, _ccpn)

					dp.AddDag(_ccpn)
					// cet cids for interims
					// dp.CurrentInterim.Cids = append(dp.CurrentInterim.Cids, _ccpn.Cid().String())
				}
				dp.CurrentInterim.Cid = _concatedChunkedProtoNodes[0].Cid().String()

				// interimProtoNodes = append(interimProtoNodes, _concatedChunkedProtoNodes[0])
				// dp.AddDag(_concatedChunkedProtoNodes[0])
				// dp.CurrentInterim.Cid = _concatedChunkedProtoNodes[0].Cid().String()

				_chunkedProtoNodes = []ipld.Node{}
				_currentSize = 0

				// Append to CurrentFolder and then Recreate Interim block
				dp.CurrentFolder.Nodes = append(dp.CurrentFolder.Nodes, *dp.CurrentInterim)
				dp.CurrentInterim = &Node{
					Path: fmt.Sprintf("%v/interim_%v", d.Name(), len(dp.CurrentFolder.Nodes)+1),
				}
			}

			// cleanup
			runtime.GC()
			debug.FreeOSMemory()

			dp.DisplayProgress(true)
		}
		// Print recently collected heap statistics
		// fmt.Println(len(_chunkedProtoNodes), len(interimProtoNodes))
		// 2. if there are leftovers files and there interims present, then pack leftovers to interim
		if len(_chunkedProtoNodes) > 1 && len(interimProtoNodes) > 0 {
			// fmt.Println("CASE 1 Concatenating interims")
			_concatedChunkedProtoNodes, err := dp.UnixfsCat.ConcatFileNodes(_chunkedProtoNodes...)
			if err != nil {
				log.Fatal(err)
			}

			for _, _ccpn := range _concatedChunkedProtoNodes {
				dp.AddDag(_ccpn)
				interimProtoNodes = append(interimProtoNodes, _ccpn)
				// dp.CurrentInterim.Cids = append(dp.CurrentInterim.Cids, _ccpn.Cid().String())
			}
			dp.CurrentInterim.Cid = _concatedChunkedProtoNodes[0].Cid().String()

			// Append to CurrentFolder and then Recreate Interim block
			dp.CurrentFolder.Nodes = append(dp.CurrentFolder.Nodes, *dp.CurrentInterim)
			dp.CurrentInterim = &Node{
				Path: fmt.Sprintf("%v/interim_%v", d.Name(), len(dp.CurrentFolder.Nodes)+1),
			}

			_concatedFileNodes, err := dp.UnixfsCat.ConcatFileNodes(interimProtoNodes...)
			if err != nil {
				log.Fatal(err)
			}

			for len(_concatedFileNodes) > 1 {
				var ipldNodes []ipld.Node
				for _, node := range _concatedFileNodes {
					ipldNodes = append(ipldNodes, node)
					dp.AddDag(node)
				}

				_concatedFileNodes, err = dp.UnixfsCat.ConcatFileNodes(ipldNodes...)
				if err != nil {
					log.Fatal(err)
				}
				for _, _ccpn := range _concatedFileNodes {
					dp.AddDag(_ccpn)
				}
			}

			dp.SetNodesWithName(_concatedFileNodes[0], d.Name())

			dp.AddDag(_concatedFileNodes[0])
			// dp.CurrentFolder.Cids = append(dp.CurrentFolder.Cids, _concatedFileNodes[0].Cid().String())
			dp.CurrentFolder.Cid = _concatedFileNodes[0].Cid().String()
			// for _, _cfn := range _concatedFileNodes {
			// 	dp.SetNodesWithName(_cfn, d.Name())
			// 	dp.AddDag(_cfn)

			// 	dp.CurrentFolder.Cids = append(dp.CurrentFolder.Cids, _cfn.Cid().String())
			// }

			_chunkedProtoNodes, interimProtoNodes = []ipld.Node{}, []ipld.Node{}
		}
		// 3. if there are leftovers files and no interims, then pack leftovers to Named
		if len(_chunkedProtoNodes) > 1 && len(interimProtoNodes) == 0 {
			// fmt.Println("CASE 2 Concatenating interims")
			_concatedFileNodes, err := dp.UnixfsCat.ConcatFileNodes(_chunkedProtoNodes...)
			if err != nil {
				log.Fatal(err)
			}

			// for _, _cfn := range _concatedFileNodes {
			// dp.SetNodesWithName(_cfn, d.Name())
			// dp.AddDag(_cfn)

			// dp.CurrentFolder.Cids = append(dp.CurrentFolder.Cids, _cfn.Cid().String())
			// }

			for len(_concatedFileNodes) > 1 {
				var ipldNodes []ipld.Node
				for _, node := range _concatedFileNodes {
					ipldNodes = append(ipldNodes, node)
					dp.AddDag(node)
				}

				_concatedFileNodes, err = dp.UnixfsCat.ConcatFileNodes(ipldNodes...)
				if err != nil {
					log.Fatal(err)
				}
				for _, _ccpn := range _concatedFileNodes {
					dp.AddDag(_ccpn)
				}
			}
			dp.SetNodesWithName(_concatedFileNodes[0], d.Name())

			dp.AddDag(_concatedFileNodes[0])
			// dp.CurrentFolder.Cids = append(dp.CurrentFolder.Cids, _concatedFileNodes[0].Cid().String())
			dp.CurrentFolder.Cid = _concatedFileNodes[0].Cid().String()

			// for _, _cfn := range _concatedFileNodes {
			// 	dp.SetNodesWithName(_cfn, d.Name())
			// 	dp.AddDag(_cfn)
			// 	dp.CurrentFolder.Cids = append(dp.CurrentFolder.Cids, _cfn.Cid().String())
			// }
			_chunkedProtoNodes = []ipld.Node{}
		}
		// 4. if there is only one file and no interims, make this file a NamedNode
		if len(_chunkedProtoNodes) == 1 && len(interimProtoNodes) == 0 {
			// fmt.Println("CASE 3 Concatenating interims")
			_concatedFileNodes, err := dp.UnixfsCat.ConcatFileNodes(_chunkedProtoNodes...)
			if err != nil {
				log.Fatal(err)
			}

			dp.SetNodesWithName(_concatedFileNodes[0], d.Name())
			dp.AddDag(_concatedFileNodes[0])
			// dp.CurrentFolder.Cids = append(dp.CurrentFolder.Cids, _chunkedProtoNodes[0].Cid().String())
			dp.CurrentFolder.Cid = _concatedFileNodes[0].Cid().String()
			_chunkedProtoNodes = []ipld.Node{}
		}
		// 5. if there is only one file and interims are present, then pack this file togethere with interims and then interims to NamedNodes
		if len(_chunkedProtoNodes) == 1 && len(interimProtoNodes) > 0 {
			// fmt.Println("CASE 4 Concatenating interims")
			interimProtoNodes = append(interimProtoNodes, _chunkedProtoNodes[0].(*merkledag.ProtoNode))
			// dp.CurrentInterim.Cids = append(dp.CurrentInterim.Cids, _chunkedProtoNodes[0].Cid().String())
			dp.CurrentInterim.Cid = _chunkedProtoNodes[0].Cid().String()

			dp.CurrentFolder.Nodes = append(dp.CurrentFolder.Nodes, *dp.CurrentInterim)

			_concatedFileNodes, err := dp.UnixfsCat.ConcatFileNodes(interimProtoNodes...)
			if err != nil {
				log.Fatal(err)
			}

			// for _, _cfn := range _concatedFileNodes {
			// dp.SetNodesWithName(_cfn, d.Name())
			// dp.AddDag(_cfn)

			// dp.CurrentFolder.Cids = append(dp.CurrentFolder.Cids, _cfn.Cid().String())
			// }

			for len(_concatedFileNodes) > 1 {
				var ipldNodes []ipld.Node
				for _, node := range _concatedFileNodes {
					ipldNodes = append(ipldNodes, node)
					dp.AddDag(node)
				}

				_concatedFileNodes, err = dp.UnixfsCat.ConcatFileNodes(ipldNodes...)
				if err != nil {
					log.Fatal(err)
				}
				for _, _ccpn := range _concatedFileNodes {
					dp.AddDag(_ccpn)
				}
			}
			dp.SetNodesWithName(_concatedFileNodes[0], d.Name())

			dp.AddDag(_concatedFileNodes[0])
			// dp.CurrentFolder.Cids = append(dp.CurrentFolder.Cids, _concatedFileNodes[0].Cid().String())
			dp.CurrentFolder.Cid = _concatedFileNodes[0].Cid().String()

			// for _, _cfn := range _concatedFileNodes {
			// 	dp.SetNodesWithName(_cfn, d.Name())
			// 	dp.AddDag(_cfn)
			// 	dp.CurrentFolder.Cids = append(dp.CurrentFolder.Cids, _cfn.Cid().String())
			// }
			_chunkedProtoNodes, interimProtoNodes = []ipld.Node{}, []ipld.Node{}
		}
		// 6. if there are no files left and interims are present, concat them to NamedNode
		if len(_chunkedProtoNodes) == 0 && len(interimProtoNodes) > 0 {
			// fmt.Println("CASE 5 Concatenating interims")
			_concatedFileNodes, err := dp.UnixfsCat.ConcatFileNodes(interimProtoNodes...)
			if err != nil {
				log.Fatal(err)
			}

			// for _, _cfn := range _concatedFileNodes {
			// dp.SetNodesWithName(_cfn, d.Name())
			// dp.AddDag(_cfn)

			// dp.CurrentFolder.Cids = append(dp.CurrentFolder.Cids, _cfn.Cid().String())
			// }

			for len(_concatedFileNodes) > 1 {
				var ipldNodes []ipld.Node
				for _, node := range _concatedFileNodes {
					ipldNodes = append(ipldNodes, node)
					dp.AddDag(node)
				}

				_concatedFileNodes, err = dp.UnixfsCat.ConcatFileNodes(ipldNodes...)
				if err != nil {
					log.Fatal(err)
				}
				for _, _ccpn := range _concatedFileNodes {
					dp.AddDag(_ccpn)
				}
			}
			dp.SetNodesWithName(_concatedFileNodes[0], d.Name())

			dp.AddDag(_concatedFileNodes[0])
			// dp.CurrentFolder.Cids = append(dp.CurrentFolder.Cids, _concatedFileNodes[0].Cid().String())
			dp.CurrentFolder.Cid = _concatedFileNodes[0].Cid().String()

			// for _, _cfn := range _concatedFileNodes {
			// 	dp.SetNodesWithName(_cfn, d.Name())
			// 	dp.AddDag(_cfn)
			// 	dp.CurrentFolder.Cids = append(dp.CurrentFolder.Cids, _cfn.Cid().String())
			// }
		}

		dp.ParentNode.Nodes = append(dp.ParentNode.Nodes, dp.CurrentFolder)

		runtime.GC()
		debug.FreeOSMemory()
	}

	return nil
}

func (dp *Dataprepper) _fileToProtoNode(file *os.File) ([]*merkledag.ProtoNode, error) {
	var nodes []ipld.Node

	reader := bufio.NewReader(file)

	_currentBytes := 0

	var protoNodes []*merkledag.ProtoNode
	var _logger_chunks, _logger_interims []Node

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	fileSize := fileInfo.Size()
	_needsToSplitToInterim := fileSize > dp.ProtoNodesBreakPoint

	var _logger_interim *Node
	if _needsToSplitToInterim {
		_logger_interim = &Node{
			Path: fmt.Sprintf("%v/interim_%v", filepath.Base(dp.CurrentNode.Path), 1),
		}
	}

	_c := 1
	_i := 2
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
			Path: fmt.Sprintf("chunk_%v", _c),
		}

		// _logger_chunk.Cid = node.Cid().String()
		// _logger_chunk.Cids = append(_logger_chunk.Cids, node.Cid().String())
		_logger_chunk.Cid = node.Cid().String()
		_logger_chunks = append(_logger_chunks, _logger_chunk)

		// dp.CurrentNode.Nodes = append(dp.CurrentNode.Nodes, _nodeLogger)

		_currentBytes += n

		/*
			1. if file_size <= chunk_size -> no interim, just return the file_node, it will become part of the FileFolder or the FileFolder
			2. if file_size > chunk_size -> split file to chunks, concat them to interim when enough collected, Interim will become part of the FileFolder or the FileFolder
				2.1 if _currentBytes (total current size of chunks of a file) < protonodesbreakpoint -> not enough chunks collected, if there are no more bytes in file, concat given chunks to interim. Interim will become part of the FileFolder or the FileFolder
				2.2 if _currentBytes (total current size of chunks of a file) >= protonodesbreakpoint -> enough chunks collected, concat them to interim, look for more chunks, collect them again. Interim CIDs array is returned as blocks of a file.
		*/

		// 1. if file_size strictly more than pnbp
		if int64(_currentBytes) >= dp.ProtoNodesBreakPoint && _needsToSplitToInterim {

			_chunkedProtoNodes, err := dp.UnixfsCat.ConcatFileNodes(nodes...)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Println("count interims_file", len(_chunkedProtoNodes))

			// if len(_chunkedProtoNodes) == 1 {
			// 	_logger_interim.Cid = _chunkedProtoNodes[0].Cid().String()
			// } else {

			// }

			for _, _cpn := range _chunkedProtoNodes {
				dp.AddDag(_cpn)
				protoNodes = append(protoNodes, _cpn)

				// _logger_interim.Cids = append(_logger_interim.Cids, _cpn.Cid().String())
			}
			_logger_interim.Cid = _chunkedProtoNodes[0].Cid().String()

			_logger_interim.Nodes = _logger_chunks
			_logger_chunks = []Node{}
			_logger_interims = append(_logger_interims, *_logger_interim)

			_logger_interim = &Node{
				Path: fmt.Sprintf("interim_%v", _i),
			}

			nodes = []ipld.Node{}
			_currentBytes = 0
			_i++
		}

		currentChunkSize := dp.FileChunkSize
		if int64(n) < dp.FileChunkSize {
			currentChunkSize = int64(n)
		}
		dp.Progress.ProcessedSize += currentChunkSize
		dp.DisplayProgress(false)
		_c++
	}
	// 2. if there are leftovers chunks and there interims present, then pack leftovers to interim
	if len(nodes) > 1 && len(protoNodes) > 0 {
		_concatedChunkedProtoNodes, err := dp.UnixfsCat.ConcatFileNodes(nodes...)
		if err != nil {
			log.Fatal(err)
		}

		for _, _ccpn := range _concatedChunkedProtoNodes {
			dp.AddDag(_ccpn)
			protoNodes = append(protoNodes, _ccpn)

			// _logger_interim.Cids = append(_logger_interim.Cids, _ccpn.Cid().String())
		}
		_logger_interim.Cid = _concatedChunkedProtoNodes[0].Cid().String()
		// reset to not trigger following ifs
		_logger_interim.Nodes = _logger_chunks
		_logger_chunks = []Node{}
		_logger_interims = append(_logger_interims, *_logger_interim)

		_logger_interim = &Node{
			Path: fmt.Sprintf("interim_%v", _i),
		}

		nodes = []ipld.Node{}
	}

	// 3. if there are leftovers chunks and no interims, then just return them
	// 4. if there is only one chunk and interims are present, then add this chunk to interims and return
	// 5. if there is only one chunk and no interims are present, then add this chunk to interims and return
	if len(nodes) >= 1 && len(protoNodes) >= 0 {
		for _, _n := range nodes {
			protoNodes = append(protoNodes, _n.(*merkledag.ProtoNode))

			_logger_interim = &Node{
				Path: fmt.Sprintf("chunk_%v", _c),
				Cid:  _n.Cid().String(),
				// Cid: []string{_n.Cid().String()},
			}
			_c++
		}
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
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	allocMB := bToMb(m.Alloc)

	// Truncate the current file path if it's too long
	currentFile := dp.Progress.CurrentFile
	if len(currentFile) > 50 {
		currentFile = "..." + currentFile[len(currentFile)-47:]
	}

	progressStr := fmt.Sprintf("\rProcessing %v: %.2f%% | Alloc: %v MiB | NumGC: %v",
		currentFile, percent, allocMB, m.NumGC)

	// Calculate available terminal width
	termWidth, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		termWidth = 120 // Default width if unable to get terminal size
	}

	// Truncate or pad the progress string to fit the terminal width
	if len(progressStr) > termWidth {
		progressStr = progressStr[:termWidth-3] + "..."
	} else {
		padding := termWidth - len(progressStr)
		if padding > 0 {
			progressStr += strings.Repeat(" ", padding)
		}
	}

	fmt.Print(progressStr)
	os.Stdout.Sync()
}
