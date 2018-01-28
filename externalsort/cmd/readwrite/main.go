package main

import (
	"externalsort/pipeline"
	"os"
	"fmt"
	"bufio"
)

// get the random data
func main() {
	const filename = "small.in"
	const n = 512
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()	// 保证运行

	p := pipeline.RandomSource(n)

	writer := bufio.NewWriter(file)
	pipeline.WriterSink(writer, p)
	writer.Flush()

	file, err = os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()	// 保证运行

	p = pipeline.ReaderSource(bufio.NewReader(file), -1)
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count >= 100 {
			break
		}
	}
}
