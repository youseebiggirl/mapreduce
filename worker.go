package mapreduce

import "net/rpc"

type (
	WorkerStatus int64
	WorkerType   int64
)

const (
	_ WorkerStatus = iota

	Idle          // 初始
	Running       // 运行中
	FinishSuccess // 运行完成（结果成功）
	FinishFail    // 运行完成（结果失败）
)

type Worker struct {
	Status WorkerStatus
	Type   WorkerType
	Addr   string
	//task   Task
	Done chan struct{}
}

func NewWorker(addr string, tye WorkerType, task Task) *Worker {
	w := &Worker{
		Status: Idle,
		Addr:   addr,
		//task:   task,
		Type: tye,
		Done: make(chan struct{}),
	}
	return w
}

func (w *Worker) Do() error {
	dial, err := rpc.Dial("tcp", ":7100")
	if err != nil {
		return err
	}

	// 通过 rpc 不断获取 task
	for {
		var (
			req  = &MasterRpcArgs{}
			resp = &MasterRpcReply{}
		)

		if err := dial.Call("MasterRpc.Apply", req, resp); err != nil {
			return err
		}

		task := resp.Task
		w.Status = Running
		if err := task.Do(); err != nil {
			w.Status = FinishFail
			w.Done <- struct{}{}
			return err
		}
		w.Status = FinishSuccess
		w.Done <- struct{}{}
		return nil
	}
}

func (w *Worker) Server() error {
	// TODO
	return nil
}
