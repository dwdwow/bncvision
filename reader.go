package bncvision

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
)

func ReadCsvZipToStructs[T any](zipPath string, rawToStructFunc RawToStructFunc[T]) ([]T, error) {
	zipReader, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, err
	}
	defer zipReader.Close()

	var results []T

	if len(zipReader.File) != 1 {
		return nil, fmt.Errorf("zipReader.File must be 1, but got %d", len(zipReader.File))
	}

	file := zipReader.File[0]

	fileReader, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer fileReader.Close()

	csvReader := csv.NewReader(fileReader)
	csvReader.FieldsPerRecord = -1

	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, nil
	}

	firstRecord := records[0]
	if len(firstRecord) == 0 {
		return nil, fmt.Errorf("first record is empty")
	}

	var hasHeader bool
	_, err = rawToStructFunc(firstRecord)
	if err != nil {
		hasHeader = true
	}

	if hasHeader && len(records) == 1 {
		return nil, nil
	}

	for _, record := range records[1:] {
		result, err := rawToStructFunc(record)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}
