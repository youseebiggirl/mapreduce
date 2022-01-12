package mapreduce

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
)

type (
	MasterRpcArgs struct{}

	MasterRpcReply struct {
		Task Task
	}
)

type (
	MapWorkerRpc struct{}

	MapWorkerRpcArgs struct {
		FileSrc string
	}

	MapWorkerRpcReply struct {
		File []byte
	}
)

// ReadFile reduce 读取 map 存储在本地的结果
func (r *MapWorkerRpc) ReadFile(req *MapWorkerRpcArgs, resp *MapWorkerRpcReply) error {
	f, err := os.Open(req.FileSrc)
	if err != nil {
		return fmt.Errorf("open file[%s] error: %v", req.FileSrc, err)
	}
	defer f.Close()

	_, err = io.Copy(bytes.NewBuffer(resp.File), f)
	if err != nil {
		return fmt.Errorf("read file[%v] error: %v", req.FileSrc, err)
	}

	return nil
}

// MapWorkerRpcRun 每个 map worker 节点都需要运行该 rpc
func MapWorkerRpcRun() error {
	if err := rpc.Register(new(MapWorkerRpc)); err != nil {
		return err
	}

	lis, err := net.Listen("tcp", ":7001")
	if err != nil {
		return err
	}

	rpc.Accept(lis)
	return nil
}
