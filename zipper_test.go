package bncvision

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestUnzip(t *testing.T) {
	// Create a temporary zip file for testing
	tempDir := t.TempDir()
	zipPath := filepath.Join(tempDir, "test.zip")

	// Create test data
	testData := map[string][]byte{
		"file1.txt": []byte("Hello, World!"),
		"file2.txt": []byte("Testing Unzip function"),
	}

	// Create a zip file with test data
	if err := createTestZip(zipPath, testData); err != nil {
		t.Fatalf("Failed to create test zip file: %v", err)
	}

	// Test Unzip function
	contents, err := Unzip(zipPath)
	if err != nil {
		t.Fatalf("Unzip failed: %v", err)
	}

	// Verify the contents
	for fileName, expectedData := range testData {
		if !bytes.Equal(contents[fileName], expectedData) {
			t.Errorf("Mismatch for file %s. Expected: %s, Got: %s", fileName, expectedData, contents[fileName])
		}
	}
}

func TestIsZippedFileValid(t *testing.T) {
	// Create a temporary zip file for testing
	tempDir := t.TempDir()
	validZipPath := filepath.Join(tempDir, "valid.zip")
	invalidZipPath := filepath.Join(tempDir, "invalid.zip")

	// Create a valid zip file
	if err := createTestZip(validZipPath, map[string][]byte{"test.txt": []byte("test")}); err != nil {
		t.Fatalf("Failed to create valid test zip file: %v", err)
	}

	// Create an invalid zip file
	if err := os.WriteFile(invalidZipPath, []byte("not a zip file"), 0644); err != nil {
		t.Fatalf("Failed to create invalid test file: %v", err)
	}

	// Test valid zip file
	if err := IsZippedFileValid(validZipPath); err != nil {
		t.Errorf("IsZippedFileValid failed for valid zip: %v", err)
	}

	// Test invalid zip file
	if err := IsZippedFileValid(invalidZipPath); err == nil {
		t.Errorf("IsZippedFileValid should have failed for invalid zip")
	}
}

func TestSaveZippedData(t *testing.T) {
	tempDir := t.TempDir()
	testPath := filepath.Join(tempDir, "test.zip")
	testData := []byte("test data")

	if err := SaveZippedData(testData, testPath); err != nil {
		t.Fatalf("SaveZippedData failed: %v", err)
	}

	// Verify the file was created and contains the correct data
	savedData, err := os.ReadFile(testPath)
	if err != nil {
		t.Fatalf("Failed to read saved file: %v", err)
	}

	if !bytes.Equal(savedData, testData) {
		t.Errorf("Saved data does not match. Expected: %s, Got: %s", testData, savedData)
	}
}

func TestSaveZippedDataWithRetry(t *testing.T) {
	tempDir := t.TempDir()
	testPath := filepath.Join(tempDir, "test.zip")
	testData := []byte("test data")

	err := ZipDataAndSave(testData, "test.txt", testPath)
	if err != nil {
		t.Fatalf("ZipDataAndSave failed: %v", err)
	}

	zipdata, err := os.ReadFile(testPath)
	if err != nil {
		t.Fatalf("Failed to read saved file: %v", err)
	}

	if err := SaveZippedDataWithRetry(zipdata, testPath, 3); err != nil {
		t.Fatalf("SaveZippedDataWithRetry failed: %v", err)
	}
}

func TestZipDataAndSave(t *testing.T) {
	tempDir := t.TempDir()
	testPath := filepath.Join(tempDir, "test.zip")
	testData := []byte("test data")
	insideFileName := "test.txt"

	if err := ZipDataAndSave(testData, insideFileName, testPath); err != nil {
		t.Fatalf("ZipDataAndSave failed: %v", err)
	}

	// Verify the zip file was created and is valid
	if err := IsZippedFileValid(testPath); err != nil {
		t.Errorf("Created zip file is invalid: %v", err)
	}

	// Unzip and verify contents
	contents, err := Unzip(testPath)
	if err != nil {
		t.Fatalf("Failed to unzip created file: %v", err)
	}

	if !bytes.Equal(contents[insideFileName], testData) {
		t.Errorf("Zipped data does not match. Expected: %s, Got: %s", testData, contents[insideFileName])
	}
}

func TestZipDataAndSaveWithRetry(t *testing.T) {
	tempDir := t.TempDir()
	testPath := filepath.Join(tempDir, "test.zip")
	testData := []byte("test data")
	insideFileName := "test.txt"

	if err := ZipDataAndSaveWithRetry(testData, insideFileName, testPath, 3); err != nil {
		t.Fatalf("ZipDataAndSaveWithRetry failed: %v", err)
	}

	// Verify the zip file was created and is valid
	if err := IsZippedFileValid(testPath); err != nil {
		t.Errorf("Created zip file is invalid: %v", err)
	}

	// Unzip and verify contents
	contents, err := Unzip(testPath)
	if err != nil {
		t.Fatalf("Failed to unzip created file: %v", err)
	}

	if !bytes.Equal(contents[insideFileName], testData) {
		t.Errorf("Zipped data does not match. Expected: %s, Got: %s", testData, contents[insideFileName])
	}
}

// Helper function to create a test zip file
func createTestZip(zipPath string, files map[string][]byte) error {
	zipFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	for name, data := range files {
		f, err := zipWriter.Create(name)
		if err != nil {
			return err
		}
		_, err = f.Write(data)
		if err != nil {
			return err
		}
	}

	return nil
}
