package bncvision

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/sync/errgroup"
)

// SaveStructToJSON saves a slice of structs to a JSON file.
//
// Parameters:
//   - data: A slice of structs of type T to be saved.
//   - filePath: The path to the JSON file where the data will be saved.
//
// Returns:
//   - An error if any step of the saving process fails, nil otherwise.
func SaveStructToJSON[T any](data []T, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	return encoder.Encode(data)
}

// ReadCSVToStructsAndSaveToJSON reads a CSV file, converts its contents to a slice of structs,
// and saves the structs to a JSON file.
//
// Parameters:
//   - csvFilePath: The path to the CSV file to be read.
//   - jsonFilePath: The path to the JSON file where the data will be saved.
//   - convertFunc: A function that converts a single CSV row (string slice) to a struct of type T.
//
// Returns:
//   - An error if any step of the reading, conversion, or saving process fails, nil otherwise.
func ReadCSVToStructsAndSaveToJSON[T any](csvFilePath, jsonFilePath string, convertFunc RawToStructFunc[T]) error {
	data, err := ReadCSVToStructs(csvFilePath, convertFunc)
	if err != nil {
		return err
	}
	return SaveStructToJSON(data, jsonFilePath)
}

// ReadAllCSVToStructsAndSaveToJSON reads all CSV files in a directory, converts their contents to a slice of structs,
// and saves the structs to JSON files in a specified directory.
//
// Parameters:
//   - csvFileDir: The directory containing the CSV files to be read.
//   - jsonFileDir: The directory where the JSON files will be saved.
//   - convertFunc: A function that converts a single CSV row (string slice) to a struct of type T.
//
// Returns:
//   - An error if any step of the reading, conversion, or saving process fails, nil otherwise.
func ReadAllCSVToStructsAndSaveToJSON[T any](csvFileDir, jsonFileDir string, convertFunc RawToStructFunc[T]) error {
	if err := os.MkdirAll(jsonFileDir, 0o755); err != nil {
		return err
	}

	files, err := os.ReadDir(csvFileDir)
	if err != nil {
		return err
	}

	maxWorkers := runtime.NumCPU() / 2
	if maxWorkers == 0 {
		maxWorkers = 1
	}

	wg := errgroup.Group{}
	wg.SetLimit(maxWorkers)

	for _, file := range files {
		file := file
		wg.Go(func() error {
			csvFilePath := filepath.Join(csvFileDir, file.Name())
			jsonFilePath := filepath.Join(jsonFileDir, file.Name())
			jsonFilePath = strings.Replace(jsonFilePath, ".csv", ".json", 1)
			slog.Info("reading", "csvFilePath", csvFilePath, "jsonFilePath", jsonFilePath)
			err := ReadCSVToStructsAndSaveToJSON(csvFilePath, jsonFilePath, convertFunc)
			if err != nil {
				slog.Error("read", "csvFilePath", csvFilePath, "jsonFilePath", jsonFilePath, "error", err)
				return err
			}
			slog.Info("read", "csvFilePath", csvFilePath, "jsonFilePath", jsonFilePath)
			return nil
		})
	}

	return wg.Wait()
}
