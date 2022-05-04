package main

//
// a word-count application "plugin" for MapReduce.
//
// go build -buildmode=plugin wc.go
//

import "6.824/mr"
import "unicode"
import "strings"
import "strconv"

// Map
// 每个文件输入map函数都会被调用调用一次
// 第一个参数是输入文件名，第二个参数是文件的完整内容
// 应该重点关注内容参数，忽略输入文件名
// 函数的返回值是key/value对的切片
*/
func Map(filename string, contents string) []mr.KeyValue {
	// 检测单词分隔符.
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

// Reduce
// reduce函数在map任务生成每个key时候被调用，任意map任务都会生成该key对应的所有值的列表
func Reduce(key string, values []string) string {
	// 返回该单词的出现次数.
	return strconv.Itoa(len(values))
}
