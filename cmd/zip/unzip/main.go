package main

import (
	"log"

	"github.com/dwdwow/bncvision"
)

func main() {
	unzip()
}

func unzip() {
	err := bncvision.UnzipAllAndSaveInDir("/home/ubuntu/data.binance.vision/data/spot/daily/trades/BTCUSDT", "/home/ubuntu/unzip.binance.vision/data/spot/daily/trades/BTCUSDT")
	if err != nil {
		log.Fatal(err)
	}
}
