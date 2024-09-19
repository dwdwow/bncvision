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
	var intervals []int64
	var quote float64
	var preInterval int64
	for i, trade := range trades {
		if i == 0 {
			preInterval = trade.Time
			continue
		}
		quote += trade.QuoteQty
		if quote > 1_000_000 {
			interval := trade.Time - preInterval
			intervals = append(intervals, interval)
			quote = 0
			preInterval = trade.Time
		}
	}
	fmt.Println("intervals calculated")
	fmt.Println(intervals)
	fmt.Println("Saving intervals to file")
	err = saveIntervalsToJSON(intervals, "/home/ubuntu/work.binance.vision/test/intervals.json")
	if err != nil {
		panic(err)
	}
	fmt.Println("Intervals saved successfully")
}

func saveIntervalsToJSON(intervals []int64, filePath string) error {
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
