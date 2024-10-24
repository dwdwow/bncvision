package bncvision

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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

func VerifyOneDirAggTradesContinuity(dir string, maxCpus int) error {
	if maxCpus <= 0 {
		maxCpus = 1
	}

	var validFiles []string
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".csv") {
			validFiles = append(validFiles, file.Name())
		}
	}

	sort.Slice(validFiles, func(i, j int) bool {
		return validFiles[i] < validFiles[j]
	})

	wg := errgroup.Group{}
	wg.SetLimit(maxCpus)

	lastIds := make([][2]int64, len(validFiles))

	for i, file := range validFiles {
		i, file := i, file
		wg.Go(func() error {
			filePath := filepath.Join(dir, file)
			slog.Info("Reading CSV To Structs", "file", file)
			aggTrades, err := ReadCSVToStructsWithFilter(filePath, AggTradeRawToStruct, AggTradesReadFilter)
			if err != nil {
				slog.Error("Read CSV To Structs", "file", file, "error", err)
				return err
			}
			slog.Info("Read CSV To Structs", "file", file, "len", len(aggTrades))
			if len(aggTrades) == 0 {
				slog.Info("ReadCSVToStructs Skip", "file", file, "len", len(aggTrades))
				return nil
			}
			slog.Info("Verifying Agg Trades Continuity", "file", file)
			err = VerifyAggTradesContinues(aggTrades, 1)
			if err != nil {
				slog.Error("Verify Agg Trades Continuity", "file", file, "error", err)
				return err
			}
			slog.Info("Verified Agg Trades Continuity", "file", file)
			lastIds[i] = [2]int64{aggTrades[0].FirstTradeId, aggTrades[len(aggTrades)-1].LastTradeId}
			return nil
		})
	}

	err = wg.Wait()
	if err != nil {
		return err
	}

	for i, file := range validFiles[:len(validFiles)-1] {
		if lastIds[i][1]+1 != lastIds[i+1][0] {
			return fmt.Errorf("agg trade file %s and %s are not continuous", file, validFiles[i+1])
		}
	}

	return nil
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

func OneDirAggTradesToInnerDayKlines(dir string, interval time.Duration, maxCpus int) ([]*bnc.Kline, error) {
	if interval.Hours() >= 24 {
		return nil, fmt.Errorf("interval must be less than one day")
	}

	if maxCpus <= 0 {
		maxCpus = 1
	}

	err := VerifyOneDirAggTradesContinuity(dir, maxCpus)
	if err != nil {
		return nil, err
	}

	var validFiles []string
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".csv") {
			validFiles = append(validFiles, file.Name())
		}
	}

	sort.Slice(validFiles, func(i, j int) bool {
		return validFiles[i] < validFiles[j]
	})

	wg := errgroup.Group{}
	wg.SetLimit(maxCpus)
	mu := sync.Mutex{}
	klines := []*bnc.Kline{}

	for _, file := range validFiles {
		file := file
		wg.Go(func() error {
			slog.Info("Reading CSV To Structs", "file", file)
			aggTrades, err := ReadCSVToStructsWithFilter(filepath.Join(dir, file), AggTradeRawToStruct, AggTradesReadFilter)
			if err != nil {
				slog.Error("Read CSV To Structs", "file", file, "error", err)
				return err
			}
			slog.Info("Read CSV To Structs", "file", file, "len", len(aggTrades))
			slog.Info("Merging Agg Trades To Klines", "file", file, "len", len(aggTrades))
			kl, err := AggTradesToKlines(aggTrades, interval)
			if err != nil {
				slog.Error("Merging Agg Trades To Klines", "file", file, "error", err)
				return err
			}
			slog.Info("Merged Agg Trades To Klines", "file", file, "len", len(kl))
			mu.Lock()
			klines = append(klines, kl...)
			mu.Unlock()
			return nil
		})
	}

	err = wg.Wait()
	if err != nil {
		return nil, err
	}

	if len(klines) == 0 {
		return nil, nil
	}

	sort.Slice(klines, func(i, j int) bool {
		return klines[i].OpenTime < klines[j].OpenTime
	})

	newKlines := make([]*bnc.Kline, (klines[len(klines)-1].OpenTime-klines[0].OpenTime)/int64(interval.Milliseconds())+1)

	first := klines[0]
	newKlines[0] = first

	for _, k := range klines[1:] {
		i := (k.OpenTime - first.OpenTime) / int64(interval.Milliseconds())
		newKlines[i] = k
	}

	for i, k := range newKlines {
		if k != nil {
			continue
		}
		prev := newKlines[i-1]
		newKlines[i] = &bnc.Kline{
			OpenTime:   prev.OpenTime + int64(interval.Milliseconds()),
			CloseTime:  prev.CloseTime + int64(interval.Milliseconds()),
			OpenPrice:  prev.ClosePrice,
			ClosePrice: prev.ClosePrice,
			HighPrice:  prev.ClosePrice,
			LowPrice:   prev.ClosePrice,
		}
	}

	// debug
	for i, k := range newKlines[1:] {
		if k.OpenTime != newKlines[i].CloseTime+1 {
			panic(fmt.Sprintf("OneDirAggTradesToKlines Debug: kline %d and %d are not continuous", i, i+1))
		}
	}

	return newKlines, nil
}
