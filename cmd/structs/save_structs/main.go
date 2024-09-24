package main

import "github.com/dwdwow/bncvision"

func main() {
	readTradesCSVAndSaveStructs()
}

func readTradesCSVAndSaveStructs() {
	symbol := "BTCUSDT"
	csvFileDir := "/home/ubuntu/unzip.binance.vision/data/spot/daily/trades/" + symbol
	jsonFileDir := "/home/ubuntu/struct.binance.vision/data/spot/daily/trades/" + symbol
	err := bncvision.ReadAllCSVToStructsAndSaveToJSON(csvFileDir, jsonFileDir, bncvision.SpotTradeRawToStruct)
	if err != nil {
		panic(err)
	}
}
