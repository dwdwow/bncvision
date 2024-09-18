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

	for _, record := range records {
		result, err := rawToStructFunc(record)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}
