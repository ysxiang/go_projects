package main

import (
	"os"
	"externalsort/pipeline"
	"bufio"
	"fmt"
	"strconv"
)

// 单机版外部排序pipeline：fileSize较小时候，并行处理效率低于普通排序
// fileSize非常大时，并行处理很有效
// goroutine --channel--> goroutine
// goroutine --> WriterSink ==> ReaderSource --> goroutine
//         through net                    through net
// InMemSort --> WriterSink ==> ReaderSource --> Merge

func main() {
	p := createNetworkPipeline("small.in", 512, 4)
	writeToFile(p, "small.out")
	printFile("small.out")
}

func printFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.ReaderSource(file, -1)
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count >= 100 {
			break
		}
	}
}

func writeToFile(p <-chan int, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()	// first flush then close

	pipeline.WriterSink(writer, p)
}

func createPipeline(
	filename string,
	fileSize, chunkCount int) <-chan int {
	// assume chunkSize is divisible
	chunkSize := fileSize / chunkCount
	pipeline.Init()

	sortResults := []<-chan int{}
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}

		file.Seek(int64(i * chunkSize), 0)

		source := pipeline.ReaderSource(
			bufio.NewReader(file), chunkSize)

		sortResults = append(sortResults,
			pipeline.InMemSort(source))
	}
	return pipeline.MergeN(sortResults...)
}

func createNetworkPipeline(
	filename string,
	fileSize, chunkCount int) <-chan int {
	// assume chunkSize is divisible
	chunkSize := fileSize / chunkCount
	pipeline.Init()

	sortAddr := []string{}
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}

		file.Seek(int64(i * chunkSize), 0)

		source := pipeline.ReaderSource(
			bufio.NewReader(file), chunkSize)

		addr := ":" + strconv.Itoa(7000 + i)
		pipeline.NetworkSink(addr,
			pipeline.InMemSort(source))
		sortAddr = append(sortAddr, addr)
	}

	sortResults := []<-chan int{}
	for _, addr := range sortAddr {
		sortResults = append(sortResults,
			pipeline.NetworkSource(addr))
	}
	return pipeline.MergeN(sortResults...)
}