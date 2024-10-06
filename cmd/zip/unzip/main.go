package main

import (
	"log"

	"github.com/dwdwow/bncvision"
)

func main() {
	unzip()
}

func unzip() {
	var err error
	// dataType := "futures/um"
	dataType := "spot"
	// symbols := []string{"BTCUSDT", "ETHUSDT", "ETHBTC", "PEPEUSDT", "WLDUSDT", "BNBUSDT"}
	symbols := []string{"BTCUSDT"}
	for _, symbol := range symbols {
		// err := bncvision.UnzipAllAndSaveInDir("/home/ubuntu/data.binance.vision/data/spot/daily/trades/"+symbol, "/home/ubuntu/unzip.binance.vision/data/spot/daily/trades/"+symbol)
		// if err != nil {
		// 	log.Fatal(err)
		// }
		err = bncvision.UnzipAllAndSaveInDir("/home/ubuntu/data.binance.vision/data/"+dataType+"/daily/aggTrades/"+symbol, "/home/ubuntu/unzip.binance.vision/data/"+dataType+"/daily/aggTrades/"+symbol)
		if err != nil {
			log.Fatal(err)
		}
	}
}
