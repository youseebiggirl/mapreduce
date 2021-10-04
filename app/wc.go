package app

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/zengh1/mapreduce"
)

func Map(filename string, contents string) []*mapreduce.Pair {
	// function to detect word separators.
	// 当前字符不是字母时，说明已经读到了一个单词，比如：
	// my name is a, and her name is b.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	var kva []*mapreduce.Pair
	for _, w := range words {
		kv := &mapreduce.Pair{
			Key: w,
			Val: "1",
		}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	return fmt.Sprintf("%v:%v \n", key, strconv.Itoa(len(values)))
}

func Run() {
	m := mapreduce.NewMr(Map, Reduce)
	m.Run("xxx.txt").Sync("mr-out-0")
}
