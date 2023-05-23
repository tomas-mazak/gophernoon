package csvsort

import (
	"encoding/csv"
	"io"
	"os"
	"sort"
	"strconv"
)

// If the whole dataset to be sorted cannot fit into memory, we can use this slower but more memory efficient method:
// We read the dataset in chunks, sort each chunk, and write the sorted chunks to temporary files. Then we merge the
// sorted chunks into a single sorted dataset.
func TempFileMergesort(r *csv.Reader, w *csv.Writer, chunkSize int, tempDirectory string, lessFunc func(a, b []string) bool) {
	var eof bool
	buffer := make([][]string, chunkSize)
	chunks := 0

	for {
		// Load chunk into memory
		for i := 0; i < chunkSize; i++ {
			record, err := r.Read()
			if err != nil {
				if err == io.EOF {
					buffer = buffer[:i]
					eof = true
					break
				}
				panic(err)
			}
			buffer[i] = record
		}

		// Sort records in chunk
		sort.Slice(buffer, func(i, j int) bool {
			return lessFunc(buffer[i], buffer[j])
		})

		// Write sorted records to CSV file
		outputFile := tempDirectory + "/chunk" + strconv.Itoa(chunks) + ".csv"
		o, err := os.Create(outputFile)
		if err != nil {
			panic(err)
		}
		w := csv.NewWriter(o)
		err = w.WriteAll(buffer)
		if err != nil {
			o.Close()
			panic(err)
		}
		w.Flush()
		o.Close()

		chunks++

		if eof {
			break
		}
	}

	// Initiate the communication channels
	channels := make([]chan []string, chunks)
	eofChans := make([]chan bool, chunks)
	eofs := make([]bool, chunks)

	// Spawn goroutines to read each chunk
	for i := 0; i < chunks; i++ {
		channels[i] = make(chan []string)
		eofChans[i] = make(chan bool)
		inputFile := tempDirectory + "/chunk" + strconv.Itoa(i) + ".csv"
		go readChunk(channels[i], eofChans[i], inputFile)
	}

	sortBuffer := make([][]string, chunks)

	// Loop until all chunks have been read for the first time
	for i := 0; i < chunks; i++ {
		select {
		case record := <-channels[i]:
			sortBuffer[i] = record
		case eofs[i] = <-eofChans[i]:
		}
	}

	for {
		// Find the smallest record
		min := -1
		for i := 0; i < chunks; i++ {
			if eofs[i] {
				continue
			}
			if min == -1 || lessFunc(sortBuffer[i], sortBuffer[min]) {
				min = i
			}
		}

		// If all chunks have been read, we're done
		if min == -1 {
			break
		}

		// Write the smallest record
		w.Write(sortBuffer[min])

		// Read the next record from the chunk that contained the smallest record
		select {
		case record := <-channels[min]:
			sortBuffer[min] = record
		case eofs[min] = <-eofChans[min]:
		}
	}

	w.Flush()

	// Delete temporary files
	for i := 0; i < chunks; i++ {
		tmpFile := tempDirectory + "/chunk" + strconv.Itoa(i) + ".csv"
		// best effort - we ignor errors here
		os.Remove(tmpFile)
	}
}

func readChunk(ch chan []string, eof chan bool, inputFile string) {
	f, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				eof <- true
				break
			}
			panic(err)
		}
		ch <- record
	}
}
