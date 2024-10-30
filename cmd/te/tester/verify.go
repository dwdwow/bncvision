package tester

import (
	"fmt"
	"time"

	"github.com/dwdwow/bncvision"
	"github.com/dwdwow/cex/bnc"
)

func VerifyOneDirAggTradesContinuity() {
	dir := "/home/ubuntu/unzip.binance.vision/data/spot/daily/aggTrades/BTCUSDT"
	maxCpus := 20
	missingIds, err := bncvision.OneDirAggTradesMissings(dir, maxCpus)
	if err != nil {
		panic(err)
	}
	for _, missing := range missingIds {
		fmt.Println(
			time.UnixMilli(missing.StartTime).Format(time.RFC3339Nano),
			time.UnixMilli(missing.EndTime).Format(time.RFC3339Nano),
			missing.StartId,
			missing.EndId,
		)
	}
}

func ScanOneDirAggTradesMissingsAndDownload() {
	aggTradesDir := "/home/ubuntu/unzip.binance.vision/data/spot/daily/aggTrades/BTCUSDT"
	saveDir := "/home/ubuntu/missing.binance.vision/data/spot/daily/aggTrades/BTCUSDT"
	symbol := "BTCUSDT"
	tradesType := bnc.AggTradesTypeSpot
	maxCpus := 20
	err := bncvision.ScanOneDirAggTradesMissingsAndDownload(aggTradesDir, saveDir, symbol, tradesType, maxCpus)
	if err != nil {
		panic(err)
	}
}
