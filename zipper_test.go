package bncvision

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestUnzip(t *testing.T) {
	// Create a temporary zip file for testing
	tempDir, err := os.MkdirTemp("", "unzip_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	zipPath := filepath.Join(tempDir, "test.zip")
	content := []byte("test content")
	err = ZipAndSave(content, "test.txt", zipPath)
	if err != nil {
		t.Fatalf("Failed to create test zip: %v", err)
	}

	// Test Unzip function
	contents, err := Unzip(zipPath)
	if err != nil {
		t.Fatalf("Unzip failed: %v", err)
	}

	if len(contents) != 1 {
		t.Errorf("Expected 1 file in zip, got %d", len(contents))
	}

	if !bytes.Equal(contents["test.txt"], content) {
		t.Errorf("Content mismatch. Expected %s, got %s", content, contents["test.txt"])
	}
}

func TestUnzipAndSave(t *testing.T) {
	// Create a temporary zip file and directory for testing
	tempDir, err := os.MkdirTemp("", "unzip_save_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	zipPath := filepath.Join(tempDir, "test.zip")
	content := []byte("test content")
	err = ZipAndSave(content, "test.txt", zipPath)
	if err != nil {
		t.Fatalf("Failed to create test zip: %v", err)
	}

	destDir := filepath.Join(tempDir, "extracted")

	// Test UnzipAndSave function
	err = UnzipAndSave(zipPath, destDir)
	if err != nil {
		t.Fatalf("UnzipAndSave failed: %v", err)
	}

	// Check if file was extracted correctly
	extractedContent, err := os.ReadFile(filepath.Join(destDir, "test.txt"))
	if err != nil {
		t.Fatalf("Failed to read extracted file: %v", err)
	}

	if !bytes.Equal(extractedContent, content) {
		t.Errorf("Content mismatch. Expected %s, got %s", content, extractedContent)
	}
}

func TestIsZipValid(t *testing.T) {
	// Create a temporary zip file for testing
	tempDir, err := os.MkdirTemp("", "zip_valid_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	zipPath := filepath.Join(tempDir, "test.zip")
	content := []byte("test content")
	err = ZipAndSave(content, "test.txt", zipPath)
	if err != nil {
		t.Fatalf("Failed to create test zip: %v", err)
	}

	// Test IsZipValid function
	err = IsZipValid(zipPath)
	if err != nil {
		t.Errorf("IsZipValid failed for a valid zip: %v", err)
	}

	// Test with an invalid file
	invalidPath := filepath.Join(tempDir, "invalid.zip")
	err = os.WriteFile(invalidPath, []byte("not a zip file"), 0644)
	if err != nil {
		t.Fatalf("Failed to create invalid file: %v", err)
	}

	err = IsZipValid(invalidPath)
	if err == nil {
		t.Errorf("IsZipValid should have failed for an invalid zip")
	}
}

func TestSaveZip(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "save_zip_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test data
	content := []byte("test content")
	zipPath := filepath.Join(tempDir, "test.zip")

	// Test SaveZip function
	err = SaveZip(content, zipPath)
	if err != nil {
		t.Fatalf("SaveZip failed: %v", err)
	}

	// Verify the zip file was created
	if _, err := os.Stat(zipPath); os.IsNotExist(err) {
		t.Errorf("Zip file was not created")
	}

	// Read the content of the saved file
	savedContent, err := os.ReadFile(zipPath)
	if err != nil {
		t.Fatalf("Failed to read saved file: %v", err)
	}

	// Verify the content of the saved file
	if !bytes.Equal(savedContent, content) {
		t.Errorf("Content mismatch. Expected %s, got %s", content, savedContent)
	}

	// Test SaveZip with nested directories
	nestedPath := filepath.Join(tempDir, "nested", "dir", "test.zip")
	err = SaveZip(content, nestedPath)
	if err != nil {
		t.Fatalf("SaveZip failed with nested directories: %v", err)
	}

	// Verify the nested zip file was created
	if _, err := os.Stat(nestedPath); os.IsNotExist(err) {
		t.Errorf("Nested zip file was not created")
	}
}

func TestSaveZipWithRetry(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "save_zip_retry_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test data
	content := []byte("test content")
	zipPath := filepath.Join(tempDir, "test.zip")

	// Test SaveZipWithRetry function
	err = SaveZipWithRetry(content, zipPath, 3)
	if err != nil {
		t.Fatalf("SaveZipWithRetry failed: %v", err)
	}

	// Verify the zip file was created
	if _, err := os.Stat(zipPath); os.IsNotExist(err) {
		t.Errorf("Zip file was not created")
	}

	// Read the content of the saved file
	savedContent, err := os.ReadFile(zipPath)
	if err != nil {
		t.Fatalf("Failed to read saved file: %v", err)
	}

	// Verify the content of the saved file
	if !bytes.Equal(savedContent, content) {
		t.Errorf("Content mismatch. Expected %s, got %s", content, savedContent)
	}
}

func TestZipAndSave(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "zip_save_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	zipPath := filepath.Join(tempDir, "test.zip")
	content := []byte("test content")

	// Test ZipAndSave function
	err = ZipAndSave(content, "test.txt", zipPath)
	if err != nil {
		t.Fatalf("ZipAndSave failed: %v", err)
	}

	// Verify the zip file was created
	if _, err := os.Stat(zipPath); os.IsNotExist(err) {
		t.Errorf("Zip file was not created")
	}

	// Verify the content of the zip file
	unzippedContents, err := Unzip(zipPath)
	if err != nil {
		t.Fatalf("Failed to unzip the created file: %v", err)
	}

	if !bytes.Equal(unzippedContents["test.txt"], content) {
		t.Errorf("Content mismatch. Expected %s, got %s", content, unzippedContents["test.txt"])
	}
}

func TestZipAndSaveWithRetry(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "zip_save_retry_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	zipPath := filepath.Join(tempDir, "test.zip")
	content := []byte("test content")

	// Test ZipAndSaveWithRetry function
	err = ZipAndSaveWithRetry(content, "test.txt", zipPath, 3)
	if err != nil {
		t.Fatalf("ZipAndSaveWithRetry failed: %v", err)
	}

	// Verify the zip file was created
	if _, err := os.Stat(zipPath); os.IsNotExist(err) {
		t.Errorf("Zip file was not created")
	}

	// Verify the content of the zip file
	unzippedContents, err := Unzip(zipPath)
	if err != nil {
		t.Fatalf("Failed to unzip the created file: %v", err)
	}

	if !bytes.Equal(unzippedContents["test.txt"], content) {
		t.Errorf("Content mismatch. Expected %s, got %s", content, unzippedContents["test.txt"])
	}
}
