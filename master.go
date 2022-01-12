package mapreduce

import (
	"net"
	"net/rpc"
)

type Master struct {
	Addr           string
	nReduce        int64
	Tasks          map[int64]Task
	availableTasks chan Task
}

func NewMaster(addr string, files []string, nReduce int64) *Master {
	m := &Master{
		Addr:           addr,
		nReduce:        nReduce,
		Tasks:          make(map[int64]Task),
		availableTasks: make(chan Task, 10), // TODO size 10
	}

	for _, f := range files {
		task := NewTaskMap(_, f)
		m.availableTasks <- task
	}
	return m
}

func (m *Master) Done() bool {
	return false
}

// Apply 用于 worker 向 master 申请一个 task
func (m *Master) Apply(args *MasterRpcArgs, reply *MasterRpcReply) error {
	t := <-m.availableTasks
	return nil
}

// Report
func (m *Master) Report() error {
	return nil
}

func (m *Master) Server() error {
	mas := NewMaster(":7100", nil, 5)
	if err := rpc.Register(mas); err != nil {
		return err
	}

	l, err := net.Listen("tcp", mas.Addr)
	if err != nil {
		return err
	}

	rpc.Accept(l)
	return nil
}
