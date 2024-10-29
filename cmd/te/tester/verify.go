package tester

import (
	"fmt"
	"time"

	"github.com/dwdwow/bncvision"
)

func VerifyOneDirAggTradesContinuity() {
	dir := "/home/ubuntu/unzip.binance.vision/data/spot/daily/aggTrades/BTCUSDT"
	maxCpus := 20
	missingIds, err := bncvision.OneDirAggTradesMissingIDs(dir, maxCpus)
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
