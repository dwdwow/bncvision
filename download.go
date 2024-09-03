package bncvision

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

// Download downloads a file from the given URL and returns its contents as a byte slice.
//
// Parameters:
//   - url: The URL of the file to be downloaded.
//
// Returns:
//   - A byte slice containing the downloaded file's contents.
//   - An integer representing the HTTP status code of the response.
//   - An error if any step of the download process fails.
func Download(url string) ([]byte, int, error) {
	// Create a new HTTP client
	client := &http.Client{}

	// Send a GET request to the URL
	resp, err := client.Get(url)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to send GET request: %w", err)
	}
	defer resp.Body.Close()

	statusCode := resp.StatusCode

	// Read the response body
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, statusCode, fmt.Errorf("failed to read response body: %w", err)
	}

	if statusCode != http.StatusOK {
		return nil, statusCode, fmt.Errorf("failed to download file: unexpected status code %d", statusCode)
	}

	return data, statusCode, nil
}

// DownloadWithRetry downloads a file from the given URL with retry attempts.
//
// Parameters:
//   - url: The URL of the file to be downloaded.
//   - retryCount: The number of retry attempts if the initial download fails.
//
// Returns:
//   - A byte slice containing the downloaded file's contents.
//   - An integer representing the HTTP status code of the response.
//   - An error if the download process fails after all retry attempts.
func DownloadWithRetry(url string, tryCount int) ([]byte, int, error) {
	var (
		data       []byte
		statusCode int
		err        error
	)

	for i := 0; i < tryCount; i++ {
		data, statusCode, err = Download(url)
		if err == nil {
			return data, statusCode, nil
		}

		if i < tryCount {
			// Log the error and retry
			gLogger.Error("Download attempt failed, retrying", "attempt", i+1, "error", err)
		}
	}

	return nil, statusCode, fmt.Errorf("failed to download file after %d attempts: %w", tryCount+1, err)
}

// DownloadSaveZipWithRetry downloads a zip file from the given URL, saves it locally, and verifies its validity.
// If the initial attempt fails or produces an invalid zip, it retries the process.
//
// Parameters:
//   - url: The URL of the zip file to be downloaded.
//   - savePath: The local path where the zip file will be saved.
//   - tryCount: The number of retry attempts if the initial download fails or produces an invalid zip.
//
// Returns:
//   - An error if the download process fails after all retry attempts, or if the resulting file is invalid.
//   - nil if the download succeeds and produces a valid zip file.
func DownloadSaveZipWithRetry(url, savePath string, tryCount int) error {
	if tryCount <= 0 {
		return fmt.Errorf("tryCount must be greater than 0")
	}

	data, statusCode, err := DownloadWithRetry(url, tryCount)
	if err != nil {
		return fmt.Errorf("failed to download zip file: %w", err)
	}

	if statusCode != http.StatusOK {
		return fmt.Errorf("failed to download zip file: unexpected status code %d", statusCode)
	}

	err = SaveZippedDataWithRetry(data, savePath, tryCount)
	if err != nil {
		return fmt.Errorf("failed to save zip file: %w", err)
	}

	err = IsZippedFileValid(savePath)
	if err != nil {
		return fmt.Errorf("downloaded zip file is invalid: %w", err)
	}

	return nil
}

// DownloadSaveZipWithRetryAndValidate checks the local zip file, and if it's invalid or doesn't exist,
// downloads the zip file from the given URL, saves it locally, and verifies its validity.
// If the download attempt fails or produces an invalid zip, it retries the process.
//
// Parameters:
//   - url: The URL of the zip file to be downloaded.
//   - savePath: The local path where the zip file will be saved.
//   - tryCount: The number of retry attempts if the download fails or produces an invalid zip.
//
// Returns:
//   - An error if the process fails after all retry attempts, or if the resulting file is invalid.
//   - nil if the local file is valid or if the download succeeds and produces a valid zip file.
func DownloadSaveZipWithRetryAndValidate(filePath, url string, tryCount int) error {
	if tryCount <= 0 {
		return fmt.Errorf("tryCount must be greater than 0")
	}

	// First, check if the local file exists and is valid
	err := IsZippedFileValid(filePath)
	if err == nil {
		// Local file is valid, no need to download
		return nil
	}

	gLogger.Info("Local zip file is invalid or doesn't exist, attempting to download", "path", filePath)

	for i := 0; i < tryCount; i++ {
		err := DownloadSaveZipWithRetry(url, filePath, 1) // Use 1 for tryCount as we're handling retries here
		if err != nil {
			gLogger.Error("Failed to download and save zip", "attempt", i+1, "error", err)
			continue
		}

		err = IsZippedFileValid(filePath)
		if err == nil {
			// Zip file is valid, return success
			return nil
		}

		gLogger.Error("Downloaded zip file is invalid, retrying", "attempt", i+1, "error", err)
		// Delete the invalid zip file before retrying
		if err := os.Remove(filePath); err != nil {
			gLogger.Error("Failed to remove invalid zip file", "path", filePath, "error", err)
		}
	}

	return fmt.Errorf("failed to download and validate zip file after %d attempts", tryCount)
}
