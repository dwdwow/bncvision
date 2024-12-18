package main

import (
	"fmt"

	"github.com/dwdwow/bncvision"
)

func main() {
	// symbols := []string{"ETHBTC", "PEPEUSDT", "WLDUSDT", "BNBUSDT"}
	// mid := "futures/um"
	mid := "spot"
	symbols := []string{"BTCUSDT"}
	for _, symbol := range symbols {
		undownloadContents, err := bncvision.DownloadAllUnderPath("data/"+mid+"/daily/aggTrades/"+symbol, 20)
		if err != nil {
			panic(err)
		}
		for _, content := range undownloadContents {
			fmt.Println(content.Key)
		}
	}
}
