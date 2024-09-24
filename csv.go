package bncvision

import (
	"encoding/csv"
	"io"
	"os"
)

// ReadCSV reads a CSV file and returns its contents as a slice of string slices.
//
// Parameters:
//   - filePath: The path to the CSV file to be read.
//
// Returns:
//   - A slice of string slices, where each inner slice represents a row in the CSV file.
//   - An error if any step of the reading process fails, nil otherwise.
//
// This function opens the specified CSV file, reads its contents line by line,
// and stores each row as a slice of strings. It handles potential errors such as
// file not found or invalid CSV format. The function uses Go's built-in csv package
// to parse the CSV data correctly, respecting quoted fields and escape characters.
//
// Note: This function reads the entire CSV file into memory. For very large files,
// consider using a streaming approach or processing the file in chunks.
func ReadCSV(filePath string) ([][]string, error) {
	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read all records from the CSV file
	var data [][]string
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		data = append(data, record)
	}

	return data, nil
}

// CSVToStruct converts CSV data to a slice of structs using a provided conversion function.
//
// Parameters:
//   - data: A slice of string slices representing the CSV data.
//   - convertFunc: A function that converts a single CSV row (string slice) to a struct of type T.
//
// Returns:
//   - A slice of structs of type T, where each struct represents a row from the CSV data.
//   - An error if any conversion fails, nil otherwise.
//
// This function iterates through each row of the CSV data, applies the provided conversion
// function to transform the row into a struct, and collects all the resulting structs into a slice.
// It's designed to be flexible, allowing the caller to define how each row should be converted
// to a struct through the convertFunc parameter.
func CSVToStruct[T any](data [][]string, convertFunc RawToStructFunc[T]) ([]T, error) {
	var result []T

	for _, row := range data {
		item, err := convertFunc(row)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}

	return result, nil
}
