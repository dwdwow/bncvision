package main

import (
	"fmt"

	"github.com/dwdwow/bncvision"
)

func main() {
	mid := "spot"
	timeframe := "1s"
	symbols := []string{"BTCUSDT"}
	for _, symbol := range symbols {
		undownloadContents, err := bncvision.DownloadAllUnderPath("data/"+mid+"/daily/klines/"+symbol+"/"+timeframe, 20)
		if err != nil {
			panic(err)
		}
		for _, content := range undownloadContents {
			fmt.Println(content.Key)
		}
	}
}
