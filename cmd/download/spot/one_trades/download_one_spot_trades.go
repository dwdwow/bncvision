package main

import (
	"fmt"

	"github.com/dwdwow/bncvision"
)

func main() {
	// symbols := []string{"ETHBTC", "PEPEUSDT", "WLDUSDT", "BNBUSDT"}
	symbols := []string{"BTCUSDT"}
	for _, symbol := range symbols {
		undownloadContents, err := bncvision.DownloadAllUnderPath("data/spot/daily/trades/"+symbol, 20)
		if err != nil {
			panic(err)
		}
		for _, content := range undownloadContents {
			fmt.Println(content.Key)
		}
	}
}
