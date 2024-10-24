package bncvision

import (
	"fmt"
	"math"
	"time"

	"github.com/dwdwow/cex/bnc"
	"github.com/dwdwow/mathy"
	"github.com/dwdwow/props"
	"golang.org/x/sync/errgroup"
)

func VerifyAggTradesContinues(aggTrades []bnc.SpotAggTrades, maxCpus int) error {
	if maxCpus <= 0 {
		maxCpus = 1
	}

	groups := props.DivideIntoGroups(aggTrades, len(aggTrades)/maxCpus)

	for i, group := range groups[:len(groups)-1] {
		if group[len(group)-1].LastTradeId+1 != groups[i+1][0].FirstTradeId {
			return fmt.Errorf("agg trade %d and %d are not continuous", group[len(group)-1].LastTradeId, groups[i+1][0].FirstTradeId)
		}
	}

	wg := errgroup.Group{}
	wg.SetLimit(maxCpus)

	for _, group := range groups {
		group := group
		if len(group) == 0 {
			continue
		}
		wg.Go(func() error {
			for i, aggTrade := range group[:len(group)-1] {
				if aggTrade.LastTradeId+1 != group[i+1].FirstTradeId {
					return fmt.Errorf("agg trade %d and %d are not continuous", aggTrade.LastTradeId, group[i+1].FirstTradeId)
				}
			}
			return nil
		})
	}

	return wg.Wait()
}

func AggTradesToKlines(aggTrades []bnc.SpotAggTrades, interval time.Duration) ([]*bnc.Kline, error) {
	if len(aggTrades) == 0 {
		return nil, nil
	}

	if interval == 0 {
		return nil, fmt.Errorf("interval is 0")
	}

	firstAggTrade := aggTrades[0]

	startTime := time.UnixMilli(firstAggTrade.Time)

	openTime := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, time.UTC)

	if interval < time.Hour*24 {
		openTime = openTime.Add(startTime.Sub(openTime) / interval * interval)
	}

	var klines []*bnc.Kline

	kline := &bnc.Kline{
		OpenTime:   openTime.UnixMilli(),
		CloseTime:  openTime.Add(interval).UnixMilli() - 1,
		OpenPrice:  firstAggTrade.Price,
		ClosePrice: firstAggTrade.Price,
		HighPrice:  firstAggTrade.Price,
		LowPrice:   firstAggTrade.Price,
	}

	for _, aggTrade := range aggTrades {
		if aggTrade.Time > kline.CloseTime {
			klines = append(klines, kline)

			i := (aggTrade.Time-kline.OpenTime)/int64(interval) - 1
			for ; i > 0; i-- {
				openTime = openTime.Add(interval)
				kline = &bnc.Kline{
					OpenTime:   openTime.UnixMilli(),
					CloseTime:  openTime.Add(interval).UnixMilli() - 1,
					OpenPrice:  kline.ClosePrice,
					ClosePrice: kline.ClosePrice,
					HighPrice:  kline.ClosePrice,
					LowPrice:   kline.ClosePrice,
				}
				klines = append(klines, kline)
			}

			openTime = openTime.Add(interval)

			kline = &bnc.Kline{
				OpenTime:  openTime.UnixMilli(),
				CloseTime: openTime.Add(interval).UnixMilli() - 1,
				OpenPrice: aggTrade.Price,
				HighPrice: aggTrade.Price,
				LowPrice:  aggTrade.Price,
			}
		}

		kline.HighPrice = math.Max(kline.HighPrice, aggTrade.Price)
		kline.LowPrice = math.Min(kline.LowPrice, aggTrade.Price)
		kline.ClosePrice = aggTrade.Price
		kline.Volume = mathy.BN(kline.Volume).Add(mathy.BN(aggTrade.Qty)).Round(8).Float64()
		kline.QuoteAssetVolume = mathy.BN(kline.QuoteAssetVolume).Add(mathy.BN(aggTrade.Qty * aggTrade.Price)).Round(8).Float64()
		if !aggTrade.IsBuyerMaker {
			kline.TakerBuyBaseAssetVolume = mathy.BN(kline.TakerBuyBaseAssetVolume).Add(mathy.BN(aggTrade.Qty)).Round(8).Float64()
			kline.TakerBuyQuoteAssetVolume = mathy.BN(kline.TakerBuyQuoteAssetVolume).Add(mathy.BN(aggTrade.Qty * aggTrade.Price)).Round(8).Float64()
		}
	}

	klines = append(klines, kline)

	return klines, nil
}
