package bncvision

import (
	"log/slog"
	"os"
	"path/filepath"
	"runtime"

	"golang.org/x/sync/errgroup"
)

func UnzipAllAndSaveInDir(zipDir string, destDir string) error {
	files, err := os.ReadDir(zipDir)
	if err != nil {
		return err
	}

	cpus := runtime.NumCPU()

	maxWorkers := cpus / 2

	if maxWorkers == 0 {
		maxWorkers = 1
	}

	wg := errgroup.Group{}

	wg.SetLimit(maxWorkers)

	for _, file := range files {
		zipFilePath := filepath.Join(zipDir, file.Name())
		wg.Go(func() error {
			slog.Info("unzipping", "file", zipFilePath)
			err := UnzipAndSaveWithExistChecking(zipFilePath, destDir)
			slog.Info("unzipped", "file", zipFilePath)
			if err != nil {
				slog.Error("error unzipping", "file", zipFilePath, "error", err)
				return err
			}
			return nil
		})
	}

	return wg.Wait()
}
