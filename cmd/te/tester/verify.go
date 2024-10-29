package tester

import (
	"fmt"

	"github.com/dwdwow/bncvision"
)

func VerifyOneDirAggTradesContinuity() {
	dir := "/home/ubuntu/unzip.binance.vision/data/spot/daily/aggTrades/BTCUSDT"
	maxCpus := 20
	missingIds, err := bncvision.OneDirAggTradesMissingIDs(dir, maxCpus)
	if err != nil {
		panic(err)
	}
	fmt.Println(missingIds)
}
