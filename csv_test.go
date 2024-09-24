package bncvision

import (
	"encoding/csv"
	"os"
	"testing"
)

func TestReadCSV(t *testing.T) {
	// Create a temporary CSV file for testing
	tempFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	// Write test data to the CSV file
	testData := [][]string{
		{"id", "name", "age"},
		{"1", "Alice", "30"},
		{"2", "Bob", "25"},
		{"3", "Charlie", "35"},
	}
	writer := csv.NewWriter(tempFile)
	for _, row := range testData {
		if err := writer.Write(row); err != nil {
			t.Fatalf("Failed to write to CSV: %v", err)
		}
	}
	writer.Flush()
	tempFile.Close()

	// Test ReadCSV function
	result, err := ReadCSV(tempFile.Name())
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}

	// Check if the result matches the test data
	if len(result) != len(testData) {
		t.Errorf("Expected %d rows, got %d", len(testData), len(result))
	}

	for i, row := range result {
		if len(row) != len(testData[i]) {
			t.Errorf("Row %d: Expected %d columns, got %d", i, len(testData[i]), len(row))
		}
		for j, cell := range row {
			if cell != testData[i][j] {
				t.Errorf("Row %d, Column %d: Expected %s, got %s", i, j, testData[i][j], cell)
			}
		}
	}
}
