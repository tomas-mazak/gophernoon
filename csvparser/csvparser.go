package main

import (
	"encoding/csv"
	"flag"
	"os"
	"sort"

	"github.com/tomas-mazak/gophernoon/csvparser/csvsort"
)

// CSV structure:
// Index,Organization Id,Name,Website,Country,Description,Founded,Industry,Number of employees
const (
	Index             int = 0
	OrganizationId    int = 1
	Name              int = 2
	Website           int = 3
	Country           int = 4
	Description       int = 5
	Founded           int = 6
	Industry          int = 7
	NumberOfEmployees int = 8
)

// Command line flags
var (
	inputFile  string
	outputFile string
)

func init() {
	flag.StringVar(&inputFile, "f", "", "Input CSV file (Default: STDIN)")
	flag.StringVar(&outputFile, "o", "", "Output CSV file (Default: STDOUT)")
}

func main() {
	flag.Parse()
	var err error

	// Create CSV reader
	var f *os.File
	if inputFile == "" || inputFile == "-" {
		f = os.Stdin
	} else {
		f, err = os.Open(inputFile)
		if err != nil {
			panic(err)
		}
	}
	defer f.Close()
	r := csv.NewReader(f)

	// Create CSV writer
	var o *os.File
	if outputFile == "" || outputFile == "-" {
		o = os.Stdout
	} else {
		o, err = os.Create(outputFile)
		if err != nil {
			panic(err)
		}
	}
	defer o.Close()
	w := csv.NewWriter(o)

	// Read all records from CSV, sort them and write them back to CSV
	//readAllSortAndWrite(r, w)
	csvsort.TempFileMergesort(r, w, 100, "tmp", func(a, b []string) bool {
		return a[Name] < b[Name]
	})
}

func readAllSortAndWrite(r *csv.Reader, w *csv.Writer) {
	// Read all records from CSV
	records, err := r.ReadAll()
	if err != nil {
		panic(err)
	}

	// Sort records by name
	sortBy := Name
	sort.Slice(records, func(i, j int) bool {
		return records[i][sortBy] < records[j][sortBy]
	})

	// Write all records to CSV
	err = w.WriteAll(records)
	if err != nil {
		panic(err)
	}

	// Flush CSV writer
	w.Flush()
}
