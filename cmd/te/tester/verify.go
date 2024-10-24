package tester

import "github.com/dwdwow/bncvision"

func VerifyOneDirAggTradesContinuity() {
	dir := "/home/ubuntu/unzip.binance.vision/data/spot/daily/aggTrades/BTCUSDT"
	maxCpus := 20
	err := bncvision.VerifyOneDirAggTradesContinuity(dir, maxCpus)
	if err != nil {
		panic(err)
	}
}
