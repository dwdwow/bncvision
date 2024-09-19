package main

import (
	"fmt"

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
	for i, trade := range trades {
		if i == 0 {
			continue
		}
		interval := trade.Time - trades[i-1].Time
		intervals = append(intervals, interval)
	}
	fmt.Println("intervals calculated")
	fmt.Println(intervals)
}
