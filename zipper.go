package bncvision

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Unzip extracts files from a zip archive and returns their contents as a map.
//
// Parameters:
//   - filePath: The path to the zip file to be extracted.
//
// Returns:
//   - A map where keys are file names and values are the file contents as byte slices.
//   - An error if any step of the unzipping process fails.
//
// This function opens the specified zip file, reads its contents, and stores each file's
// data in a map. It handles both files and directories within the zip archive, skipping
// directories and only storing file contents. If any error occurs during the process
// (e.g., file not found, invalid zip format), the function returns nil for the map and
// the corresponding error.
func Unzip(filePath string) (map[string][]byte, error) {
	// Read the zip file
	zipFile, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer zipFile.Close()

	// Get file info
	info, err := zipFile.Stat()
	if err != nil {
		return nil, err
	}

	// Create a new zip reader
	reader, err := zip.NewReader(zipFile, info.Size())
	if err != nil {
		return nil, err
	}

	contents := make(map[string][]byte)

	for _, file := range reader.File {
		rc, err := file.Open()
		if err != nil {
			return nil, err
		}

		if !file.FileInfo().IsDir() {
			data, err := io.ReadAll(rc)
			if err != nil {
				rc.Close()
				return nil, err
			}
			contents[file.Name] = data
		}
		rc.Close()
	}

	return contents, nil
}

// UnzipAndSave extracts the contents of a zip file to a specified directory on disk.
//
// Parameters:
//   - zipFilePath: The path to the zip file to be extracted.
//   - destDir: The destination directory where the contents will be saved.
//
// Returns:
//   - An error if any step of the unzipping process fails, nil otherwise.
//
// This function opens the specified zip file, reads its contents, and saves each file
// to the specified destination directory. It handles both files and directories within
// the zip archive, creating directories as needed. If any error occurs during the process
// (e.g., file not found, invalid zip format, insufficient permissions), the function
// returns the corresponding error.
//
// Note: This function will overwrite existing files in the destination directory if they
// have the same names as files in the zip archive.
func UnzipAndSave(zipFilePath, destDir string) error {
	// Read the zip file
	zipFile, err := os.Open(zipFilePath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	// Get file info
	info, err := zipFile.Stat()
	if err != nil {
		return err
	}

	// Create a new zip reader
	reader, err := zip.NewReader(zipFile, info.Size())
	if err != nil {
		return err
	}

	// Create destination directory if it doesn't exist
	if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
		return err
	}

	for _, file := range reader.File {
		// Construct the full path for the extracted file
		path := filepath.Join(destDir, file.Name)

		if file.FileInfo().IsDir() {
			// Create directory
			os.MkdirAll(path, file.Mode())
			continue
		}

		// Create the file
		outFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
		if err != nil {
			return err
		}

		rc, err := file.Open()
		if err != nil {
			outFile.Close()
			return err
		}

		_, err = io.Copy(outFile, rc)
		rc.Close()
		outFile.Close()

		if err != nil {
			return err
		}
	}

	return nil
}

// FileExists checks if a file exists at the given path.
//
// Parameters:
//   - filePath: The path to the file to be checked.
//
// Returns:
//   - A boolean indicating whether the file exists (true) or not (false).
//   - An error if any occurred during the check, nil otherwise.
func FileExists(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// IsZippedFileValid checks if a zip file is valid by attempting to create a new zip reader.
//
// Parameters:
//   - filePath: The path to the zip file to be checked.
//
// Returns:
//   - An error if any step of the validation process fails, nil otherwise.
//
// This function opens the specified zip file, reads its contents, and stores each file's
func IsZippedFileValid(filePath string) error {
	if !strings.HasSuffix(filePath, ".zip") {
		return fmt.Errorf("file is not a zip file")
	}
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file info
	info, err := file.Stat()
	if err != nil {
		return err
	}

	// Try to create a new zip reader
	_, err = zip.NewReader(file, info.Size())
	if err != nil {
		return err
	}

	// If we reach here, it's a valid ZIP file
	return nil
}

// SaveZippedData saves a byte slice to a file, creating the necessary directories if they don't exist.
//
// Parameters:
//   - data: The byte slice containing the data to be saved.
//   - filePath: The path where the data will be saved.
//
// Returns:
//   - An error if the file creation fails, or if the directories cannot be created.
//   - nil if the file is successfully saved.
func SaveZippedData(data []byte, filePath string) error {
	err := os.MkdirAll(filepath.Dir(filePath), 0755)
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0644)
}

// SaveZippedDataWithRetry saves a byte slice to a file, creating the necessary directories if they don't exist.
// If the initial attempt fails, it retries the process.
//
// Parameters:
//   - data: The byte slice containing the data to be saved.
//   - filePath: The path where the data will be saved.
//   - tryCount: The number of try attempts.
//
// Returns:
//   - An error if the file creation fails, or if the directories cannot be created.
//   - nil if the file is successfully saved.
func SaveZippedDataWithRetry(data []byte, filePath string, tryCount int) error {
	if tryCount <= 0 {
		return fmt.Errorf("tryCount must be greater than 0")
	}
	var err error
	for i := 0; i < tryCount; i++ {
		err = SaveZippedData(data, filePath)
		if err == nil {
			return nil
		}
		gLogger.Error("Failed to save zipped data, retrying", "attempt", i+1, "error", err)
	}
	return fmt.Errorf("failed to save zipped data after %d attempts: %w", tryCount, err)
}

// ZipAndSaveWithRetry compresses the given data, saves it to a zip file, and verifies its validity.
// If the initial attempt fails or produces an invalid zip, it retries the process.
//
// Parameters:
//   - data: The byte slice containing the data to be compressed.
//   - insideFileName: The name of the file to be created inside the zip archive.
//   - zipSavePath: The path where the resulting zip file will be saved.
//   - retryCount: The number of retry attempts if the initial zip fails or produces an invalid result.
//
// Returns:
//   - An error if the zip process fails after all retry attempts, or if the resulting file is invalid.
//   - nil if the zip process succeeds and produces a valid result.
//
// This function first attempts to zip and save the data. After each attempt, it verifies
// the validity of the created zip file. If the file is invalid or the process fails,
// it retries up to 'retryCount' times. This ensures the final zip file is both
// successfully created and valid.
func ZipDataAndSave(data []byte, insideFileName, zipSavePath string) error {
	// Create a buffer to write our archive to.
	buf := new(bytes.Buffer)

	// Create a new zip archive.
	w := zip.NewWriter(buf)

	// Create a new file inside the zip archive
	f, err := w.Create(insideFileName)
	if err != nil {
		return fmt.Errorf("failed to create file in zip: %w", err)
	}

	// Write the data to the file
	_, err = f.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data to zip: %w", err)
	}

	// Close the zip writer
	err = w.Close()
	if err != nil {
		return fmt.Errorf("failed to close zip writer: %w", err)
	}

	// Write the zip data to the file
	err = os.WriteFile(zipSavePath, buf.Bytes(), 0644)
	if err != nil {
		return fmt.Errorf("failed to write zip file: %w", err)
	}

	return nil
}

// ZipDataAndSaveWithRetry compresses the given data, saves it to a zip file, and verifies its validity.
// If the initial attempt fails or produces an invalid zip, it retries the process.
//
// Parameters:
//   - data: The byte slice containing the data to be compressed.
//   - insideFileName: The name of the file to be created inside the zip archive.
//   - zipSavePath: The path where the resulting zip file will be saved.
//   - tryCount: The number of try attempts.
//
// Returns:
//   - An error if the zip process fails after all try attempts, or if the resulting file is invalid.
//   - nil if the zip process succeeds and produces a valid result.
func ZipDataAndSaveWithRetry(data []byte, insideFileName, zipSavePath string, tryCount int) error {
	if tryCount <= 0 {
		return fmt.Errorf("tryCount must be greater than 0")
	}
	var err error
	for i := 0; i < tryCount; i++ {
		err = ZipDataAndSave(data, insideFileName, zipSavePath)
		if err == nil {
			return nil
		}
		gLogger.Error("Failed to zip data and save, retrying", "attempt", i+1, "error", err)
	}
	return fmt.Errorf("failed to zip data and save after %d attempts: %w", tryCount, err)
}
