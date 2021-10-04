package mapreduce

import (
	"io"
	"os"
	"sort"
)

type MapFunc func(filename string, content string) (res []*Pair)
type ReduceFunc func(key string, values []string) (res string)

type Pair struct {
	Key string
	Val string
}

type Mr struct {
	MapFunc    MapFunc
	ReduceFunc ReduceFunc
	err        error
	reduceRes  []string // reduce 处理后的所有结果保存在此
}

func NewMr(mapFunc MapFunc, reduceFunc ReduceFunc) *Mr {
	m := &Mr{
		MapFunc:    mapFunc,
		ReduceFunc: reduceFunc,
	}
	return m
}

func (m *Mr) Run(files ...string) *Mr {
	if len(files) == 0 {
		panic("files number must gt 0")
	}

	var intermediate []*Pair // 保存 map 处理后的中间体
	for _, filename := range files {
		f, err := os.Open(filename)
		if err != nil {
			return &Mr{err: err}
		}

		content, err := io.ReadAll(f)
		if err != nil {
			return &Mr{err: err}
		}

		r := m.MapFunc(filename, string(content))

		// for _, p := range r {
		// 	log.Printf("%+v \n", p)
		// }

		intermediate = append(intermediate, r...)
		f.Close()
	}

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

		rf := m.ReduceFunc(intermediate[i].Key, values)
		m.reduceRes = append(m.reduceRes, rf)

		i = j
	}

	return m
}

func (m *Mr) Sync(path string) error {
	if m.err != nil {
		return m.err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}

	for _, s := range m.reduceRes {
		f.WriteString(s)
	}

	return nil
}
