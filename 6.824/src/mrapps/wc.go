package main

//
// a word-count application "plugin" for MapReduce.
//
// go build -buildmode=plugin wc.go
//
// 统计文本里每个单词出现的次数

import (
	"strconv"
	"strings"
	"unicode"

	"../mr"
)

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
// Map 会找出 contents 里的所有单词，并将出现次数记为 1，
// 组成 kv 对，其中 key 是单词，value 是出现次数，例如：
// {key: "how", value: "1"}
// {key: "are", value: "1"}
// {key: "you", value: "1"}
// {key: "you", value: "1"}
//
// 相同的 key 也会分开统计
// filename 好像并没用用到
func Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators.
	// 当前字符不是字母时，说明已经读到了一个单词，比如：
	// my name is a, and her name is b.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
// Reduce 会统计 key 一共出现了几次
// 参数示例：key: "you" values: ["1", "1", "1", "1", "1"]
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	// 返回 values 的长度，该长度就是 key 出现的次数
	return strconv.Itoa(len(values))
}
