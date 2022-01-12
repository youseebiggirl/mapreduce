package mapreduce

import (
	"fmt"
	"io"
	"os"
	"sort"
)

type TaskType int64 // 16868

const (
	_ TaskType = iota
	TypeMap
	TypeReduce
)

type MapFunc func(filename string, content string) (res []*Pair)
type ReduceFunc func(key string, values []string) (res string)

type Pair struct {
	Key string
	Val string
}

type Task interface {
	Id() string
	Type() TaskType
	Do() error
	Done() chan struct{}
}

// --------------------- TaskMap ---------------------

type TaskMap struct {
	id     string
	typ    TaskType
	fn     MapFunc
	file   string
	result []*Pair // map 处理结果
	done   chan struct{}
}

func NewTaskMap(mapFunc MapFunc, file string) *TaskMap {
	t := &TaskMap{
		id:   fmt.Sprintf("%"),
		typ:  TypeMap,
		fn:   mapFunc,
		file: file,
	}
	return t
}

func (t *TaskMap) Id() string {
	return t.id
}

func (t *TaskMap) Type() TaskType {
	return t.typ
}

func (t *TaskMap) Result() []*Pair {
	return t.result
}

func (t *TaskMap) Done() chan struct{} {
	return t.done
}

func (t *TaskMap) Do() error {
	f, err := os.Open(t.file)
	if err != nil {
		return err
	}

	content, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	r := t.fn(t.file, string(content))
	t.result = append(t.result, r...)
	f.Close()

	t.done <- struct{}{}
	return nil
}

// --------------------- TaskReduce ---------------------

type TaskReduce struct {
	id        int64
	typ       TaskType
	fn        ReduceFunc
	mapResult []*Pair // map 处理结果
	result    []string
	done      chan struct{}
}

func NewTaskReduce(reduceFunc ReduceFunc, mapResult []*Pair) *TaskReduce {
	t := &TaskReduce{
		typ:       TypeReduce,
		fn:        reduceFunc,
		mapResult: mapResult,
	}
	return t
}

func (t *TaskReduce) Id() int64 {
	return t.id
}

func (t *TaskReduce) Type() TaskType {
	return t.typ
}

func (t *TaskReduce) Done() chan struct{} {
	return t.done
}

func (t *TaskReduce) Result() []string {
	return t.result
}

func (t *TaskReduce) Do() error {
	intermediate := t.mapResult
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) &&
			intermediate[i].Key == intermediate[j].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Val)
		}

		rf := t.fn(intermediate[i].Key, values)
		t.result = append(t.result, rf)

		i = j
	}

	t.done <- struct{}{}
	return nil
}
