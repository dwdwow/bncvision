package bncvision

import (
	"encoding/csv"
	"io"
	"os"

	"github.com/dwdwow/cex/bnc"
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

// CSVToStructs converts CSV data to a slice of structs using a provided conversion function.
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
func CSVToStructs[T any](data [][]string, convertFunc RawToStructFunc[T]) ([]T, error) {
	var result []T

	if len(data) == 0 {
		return result, nil
	}

	var start int

	_, err := convertFunc(data[0])
	if err != nil {
		if len(data) == 1 {
			return nil, err
		}
		start = 1
	}

	for _, row := range data[start:] {
		item, err := convertFunc(row)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}

	return result, nil
}

// CSVToStructsWithFilter converts CSV data to a slice of structs using a provided conversion function and a filter function.
//
// Parameters:
//   - data: A slice of string slices representing the CSV data.
//   - convertFunc: A function that converts a single CSV row (string slice) to a struct of type T.
//   - filterFunc: A function that filters the structs based on a condition.
//
// Returns:
//   - A slice of structs of type T, where each struct represents a row from the CSV data that passes the filter.
//   - An error if any conversion fails, nil otherwise.
func CSVToStructsWithFilter[T any](data [][]string, convertFunc RawToStructFunc[T], filter func(T) bool) ([]T, error) {
	var result []T

	if len(data) == 0 {
		return result, nil
	}

	var start int

	_, err := convertFunc(data[0])
	if err != nil {
		if len(data) == 1 {
			return nil, err
		}
		start = 1
	}

	for _, row := range data[start:] {
		item, err := convertFunc(row)
		if err != nil {
			return nil, err
		}
		if filter(item) {
			result = append(result, item)
		}
	}

	return result, nil
}

// ReadCSVToStructs reads a CSV file and converts its contents to a slice of structs using a provided conversion function.
//
// Parameters:
//   - filePath: The path to the CSV file to be read.
//   - convertFunc: A function that converts a single CSV row (string slice) to a struct of type T.
//
// Returns:
//   - A slice of structs of type T, where each struct represents a row from the CSV data.
//   - An error if any step of the reading or conversion process fails, nil otherwise.
func ReadCSVToStructs[T any](filePath string, convertFunc RawToStructFunc[T]) ([]T, error) {
	data, err := ReadCSV(filePath)
	if err != nil {
		return nil, err
	}
	return CSVToStructs(data, convertFunc)
}

// ReadCSVToStructsWithFilter reads a CSV file and converts its contents to a slice of structs using a provided conversion function and a filter function.
//
// Parameters:
//   - filePath: The path to the CSV file to be read.
//   - convertFunc: A function that converts a single CSV row (string slice) to a struct of type T.
//   - filterFunc: A function that filters the structs based on a condition.
//
// Returns:
//   - A slice of structs of type T, where each struct represents a row from the CSV data that passes the filter.
//   - An error if any step of the reading or conversion process fails, nil otherwise.
func ReadCSVToStructsWithFilter[T any](filePath string, convertFunc RawToStructFunc[T], filterFunc func(T) bool) ([]T, error) {
	data, err := ReadCSV(filePath)
	if err != nil {
		return nil, err
	}
	return CSVToStructsWithFilter(data, convertFunc, filterFunc)
}

func AggTradesReadFilter(aggTrade bnc.SpotAggTrades) bool {
	return aggTrade.FirstTradeId != -1 && aggTrade.LastTradeId != -1
}
