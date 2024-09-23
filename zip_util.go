package bncvision

import (
	"os"
	"path/filepath"
)

func UnzipAllAndSaveInDir(zipDir string, destDir string) error {
	files, err := os.ReadDir(zipDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		zipFilePath := filepath.Join(zipDir, file.Name())
		err := UnzipAndSave(zipFilePath, destDir)
		if err != nil {
			return err
		}
	}

	return nil
}
