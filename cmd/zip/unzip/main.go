package main

import (
	"log"

	"github.com/dwdwow/bncvision"
)

func main() {
	unzip()
}

func unzip() {
	symbols := []string{"ETHUSDT", "ETHBTC", "PEPEUSDT", "WLDUSDT", "BNBUSDT"}
	for _, symbol := range symbols {
		err := bncvision.UnzipAllAndSaveInDir("/home/ubuntu/data.binance.vision/data/spot/daily/trades/"+symbol, "/home/ubuntu/unzip.binance.vision/data/spot/daily/trades/"+symbol)
		if err != nil {
			log.Fatal(err)
		}
	}
}
