package main

import (
	"fmt"

	"github.com/dwdwow/bncvision"
)

func main() {
	undownloadContents, err := bncvision.DownloadAllUnderPath("data/spot/daily/trades/BTCUSDT", 20)
	if err != nil {
		panic(err)
	}
	for _, content := range undownloadContents {
		fmt.Println(content.Key)
	}
}
