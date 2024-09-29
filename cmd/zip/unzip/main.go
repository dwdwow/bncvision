package main

import (
	"log"

	"github.com/dwdwow/bncvision"
)

func main() {
	unzip()
}

func unzip() {
	// symbols := []string{"BTCUSDT", "ETHUSDT", "ETHBTC", "PEPEUSDT", "WLDUSDT", "BNBUSDT"}
	symbols := []string{"BTCUSDT"}
	for _, symbol := range symbols {
		err := bncvision.UnzipAllAndSaveInDir("/home/ubuntu/data.binance.vision/data/spot/daily/trades/"+symbol, "/home/ubuntu/unzip.binance.vision/data/spot/daily/trades/"+symbol)
		if err != nil {
			log.Fatal(err)
		}
		err = bncvision.UnzipAllAndSaveInDir("/home/ubuntu/data.binance.vision/data/spot/daily/aggTrades/"+symbol, "/home/ubuntu/unzip.binance.vision/data/spot/daily/aggTrades/"+symbol)
		if err != nil {
			log.Fatal(err)
		}
	}
}
