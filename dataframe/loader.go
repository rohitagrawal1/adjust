package dataframe

import (
	"encoding/csv"
	"os"
)

type DataframeLoader interface {
	Load() (Dataframe, error)
}

type LocalCsvFileLoader struct {
	FileName string
}

func (f LocalCsvFileLoader) Load() (*Dataframe, error) {
	csvFile, err := os.Open(f.FileName)
	if err != nil {
		return nil, err
	}
	defer csvFile.Close()
	csvLines, err := csv.NewReader(csvFile).ReadAll()

	if err != nil {
		return nil, err
	}
	rows := make([]Row, 0)
	for _, line := range csvLines[1:] {
		iSlice := make([]interface{}, len(line))
		for i, f := range line {
			iSlice[i] = f
		}
		row := BasicRow{
			iSlice,
		}
		rows = append(rows, row)
	}

	return &Dataframe{rows}, nil
}
