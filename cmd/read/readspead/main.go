package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/dwdwow/bncvision"
)

func main() {
	fmt.Println("start")
	trades, err := bncvision.ReadCsvZipToStructs("/home/ubuntu/data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-09-17.zip", bncvision.SpotTradeRawToStruct)
	if err != nil {
		panic(err)
	}
	fmt.Println("calculate intervals")
	var quotes []float64
	var totalQuote float64
	preTs := trades[0].Time
	for _, trade := range trades[1:] {
		totalQuote += trade.QuoteQty
		if trade.Time-preTs >= 100 {
			quotes = append(quotes, totalQuote)
			totalQuote = 0
			preTs = trade.Time
		}
	}
	fmt.Println("intervals calculated")
	fmt.Println(quotes)
	fmt.Println("Saving intervals to file")
	err = saveIntervalsToJSON(quotes, "/home/ubuntu/work.binance.vision/test/intervals.json")
	if err != nil {
		panic(err)
	}
	fmt.Println("Intervals saved successfully")
}

func saveIntervalsToJSON(intervals []float64, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(intervals)
	if err != nil {
		return fmt.Errorf("error encoding JSON: %w", err)
	}

	return nil
}
