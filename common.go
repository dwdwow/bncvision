package bncvision

import (
	"os"
	"path/filepath"
)

const (
	DATA_VISION_URL = "https://data.binance.vision"

	// DATA_BINANCE_VISION is the directory for https://data.binance.vision
	DATA_BINANCE_VISION = "data.binance.vision"
	// TIDY_BINANCE_VISION is the directory for tidied binance vision data
	// binance vision data is not complete, and some data is missing, so we need to tidy it
	TIDY_BINANCE_VISION = "tidy.binance.vision"
	// MISS_BINANCE_VISION is the directory for missing data from binance vision
	// binance vision data is not complete, and some data is missing
	MISSING_BINANCE_VISION = "missing.binance.vision"
	// UNZIP_BINANCE_VISION is the directory for unzipped binance vision data
	UNZIP_BINANCE_VISION = "unzip.binance.vision"
	// WORK_BINANCE_VISION is the directory for working with binance vision data
	WORK_BINANCE_VISION = "work.binance.vision"
	// STRUCT_BINANCE_VISION is the directory for struct binance vision data
	// save struct data to json file for future use
	STRUCT_BINANCE_VISION = "struct.binance.vision"
)

var (
	homeDir string
	dataDir string
	tidyDir string
	missDir string
)

func init() {
	var err error
	homeDir, err = os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	dataDir = filepath.Join(homeDir, DATA_BINANCE_VISION, "data")
	tidyDir = filepath.Join(homeDir, TIDY_BINANCE_VISION, "data")
	missDir = filepath.Join(homeDir, MISSING_BINANCE_VISION, "data")
}
