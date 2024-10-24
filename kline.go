package bncvision

import (
	"errors"
	"sync"
	"time"

	"github.com/dwdwow/cex/bnc"
	"github.com/dwdwow/props"
	"golang.org/x/sync/errgroup"
)

type KlineInterval string

const (
	Kline1s  KlineInterval = "1s"
	Kline1m  KlineInterval = "1m"
	Kline3m  KlineInterval = "3m"
	Kline5m  KlineInterval = "5m"
	Kline15m KlineInterval = "15m"
	Kline30m KlineInterval = "30m"
	Kline1h  KlineInterval = "1h"
	Kline2h  KlineInterval = "2h"
	Kline4h  KlineInterval = "4h"
	Kline6h  KlineInterval = "6h"
	Kline8h  KlineInterval = "8h"
	Kline12h KlineInterval = "12h"
	Kline1d  KlineInterval = "1d"
	Kline3d  KlineInterval = "3d"
	Kline1w  KlineInterval = "1w"
	Kline1mo KlineInterval = "1mo"
)

var KlineIntervalToMilli = map[KlineInterval]int64{
	Kline1s:  1 * time.Second.Milliseconds(),
	Kline1m:  1 * time.Minute.Milliseconds(),
	Kline3m:  3 * time.Minute.Milliseconds(),
	Kline5m:  5 * time.Minute.Milliseconds(),
	Kline15m: 15 * time.Minute.Milliseconds(),
	Kline30m: 30 * time.Minute.Milliseconds(),
	Kline1h:  1 * time.Hour.Milliseconds(),
	Kline2h:  2 * time.Hour.Milliseconds(),
	Kline4h:  4 * time.Hour.Milliseconds(),
	Kline6h:  6 * time.Hour.Milliseconds(),
	Kline8h:  8 * time.Hour.Milliseconds(),
	Kline12h: 12 * time.Hour.Milliseconds(),
	Kline1d:  1 * time.Hour.Milliseconds() * 24,
	Kline3d:  3 * time.Hour.Milliseconds() * 24,
	Kline1w:  1 * time.Hour.Milliseconds() * 24 * 7,
}

var MilliToKlineInterval = map[int64]KlineInterval{
	1 * time.Second.Milliseconds():    Kline1s,
	1 * time.Minute.Milliseconds():    Kline1m,
	3 * time.Minute.Milliseconds():    Kline3m,
	5 * time.Minute.Milliseconds():    Kline5m,
	15 * time.Minute.Milliseconds():   Kline15m,
	30 * time.Minute.Milliseconds():   Kline30m,
	1 * time.Hour.Milliseconds():      Kline1h,
	2 * time.Hour.Milliseconds():      Kline2h,
	4 * time.Hour.Milliseconds():      Kline4h,
	6 * time.Hour.Milliseconds():      Kline6h,
	8 * time.Hour.Milliseconds():      Kline8h,
	12 * time.Hour.Milliseconds():     Kline12h,
	1 * 24 * time.Hour.Milliseconds(): Kline1d,
	3 * 24 * time.Hour.Milliseconds(): Kline3d,
	7 * 24 * time.Hour.Milliseconds(): Kline1w,
}

var KlineIntervalToBncKlineInterval = map[KlineInterval]bnc.KlineInterval{
	Kline1s:  bnc.KlineInterval1s,
	Kline1m:  bnc.KlineInterval1m,
	Kline3m:  bnc.KlineInterval3m,
	Kline5m:  bnc.KlineInterval5m,
	Kline15m: bnc.KlineInterval15m,
	Kline30m: bnc.KlineInterval30m,
	Kline1h:  bnc.KlineInterval1h,
	Kline2h:  bnc.KlineInterval2h,
	Kline4h:  bnc.KlineInterval4h,
	Kline6h:  bnc.KlineInterval6h,
	Kline8h:  bnc.KlineInterval8h,
	Kline12h: bnc.KlineInterval12h,
	Kline1d:  bnc.KlineInterval1d,
	Kline3d:  bnc.KlineInterval3d,
	Kline1w:  bnc.KlineInterval1w,
	Kline1mo: bnc.KlineInterval1M,
}

var BncKlineIntervalToMilli = map[bnc.KlineInterval]int64{
	bnc.KlineInterval1s:  1 * time.Second.Milliseconds(),
	bnc.KlineInterval1m:  1 * time.Minute.Milliseconds(),
	bnc.KlineInterval3m:  3 * time.Minute.Milliseconds(),
	bnc.KlineInterval5m:  5 * time.Minute.Milliseconds(),
	bnc.KlineInterval15m: 15 * time.Minute.Milliseconds(),
	bnc.KlineInterval30m: 30 * time.Minute.Milliseconds(),
	bnc.KlineInterval1h:  1 * time.Hour.Milliseconds(),
	bnc.KlineInterval2h:  2 * time.Hour.Milliseconds(),
	bnc.KlineInterval4h:  4 * time.Hour.Milliseconds(),
	bnc.KlineInterval6h:  6 * time.Hour.Milliseconds(),
	bnc.KlineInterval8h:  8 * time.Hour.Milliseconds(),
	bnc.KlineInterval12h: 12 * time.Hour.Milliseconds(),
	bnc.KlineInterval1d:  1 * 24 * time.Hour.Milliseconds(),
	bnc.KlineInterval3d:  3 * 24 * time.Hour.Milliseconds(),
	bnc.KlineInterval1w:  7 * 24 * time.Hour.Milliseconds(),
}

var MilliToBncKlineInterval = map[int64]bnc.KlineInterval{
	1 * time.Second.Milliseconds():    bnc.KlineInterval1s,
	1 * time.Minute.Milliseconds():    bnc.KlineInterval1m,
	3 * time.Minute.Milliseconds():    bnc.KlineInterval3m,
	5 * time.Minute.Milliseconds():    bnc.KlineInterval5m,
	15 * time.Minute.Milliseconds():   bnc.KlineInterval15m,
	30 * time.Minute.Milliseconds():   bnc.KlineInterval30m,
	1 * time.Hour.Milliseconds():      bnc.KlineInterval1h,
	2 * time.Hour.Milliseconds():      bnc.KlineInterval2h,
	4 * time.Hour.Milliseconds():      bnc.KlineInterval4h,
	6 * time.Hour.Milliseconds():      bnc.KlineInterval6h,
	8 * time.Hour.Milliseconds():      bnc.KlineInterval8h,
	12 * time.Hour.Milliseconds():     bnc.KlineInterval12h,
	1 * 24 * time.Hour.Milliseconds(): bnc.KlineInterval1d,
	3 * 24 * time.Hour.Milliseconds(): bnc.KlineInterval3d,
	7 * 24 * time.Hour.Milliseconds(): bnc.KlineInterval1w,
}

var BncKlineIntervalToKlineInterval = map[bnc.KlineInterval]KlineInterval{
	bnc.KlineInterval1s:  Kline1s,
	bnc.KlineInterval1m:  Kline1m,
	bnc.KlineInterval3m:  Kline3m,
	bnc.KlineInterval5m:  Kline5m,
	bnc.KlineInterval15m: Kline15m,
	bnc.KlineInterval30m: Kline30m,
	bnc.KlineInterval1h:  Kline1h,
	bnc.KlineInterval2h:  Kline2h,
	bnc.KlineInterval4h:  Kline4h,
	bnc.KlineInterval6h:  Kline6h,
	bnc.KlineInterval8h:  Kline8h,
	bnc.KlineInterval12h: Kline12h,
	bnc.KlineInterval1d:  Kline1d,
	bnc.KlineInterval3d:  Kline3d,
	bnc.KlineInterval1w:  Kline1w,
	bnc.KlineInterval1M:  Kline1mo,
}

func IsMonthFirstDay(ts int64) bool {
	t := time.UnixMilli(ts)
	return t.Day() == 1 && t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0
}

var ErrKlineIntervalNotSupported = errors.New("kline interval is not supported")

func CalKlineInterval(kline bnc.Kline) (KlineInterval, error) {
	milli := kline.CloseTime + 1 - kline.OpenTime
	interval, ok := MilliToKlineInterval[milli]
	if ok {
		return interval, nil
	}
	start := time.UnixMilli(kline.OpenTime)
	end := time.UnixMilli(kline.CloseTime)
	if !IsMonthFirstDay(kline.OpenTime) {
		return "", ErrKlineIntervalNotSupported
	}
	if start.AddDate(0, 1, 0).Compare(end.Add(time.Duration(1))) == 0 {
		return Kline1mo, nil
	}
	return "", ErrKlineIntervalNotSupported
}

// CalMissingKlineOpenTimes
// Calculate the open times of klines that are missing between startOpenTime and endOpenTime
// (startOpenTime, endOpenTime) is the time range of missing klines
func CalMissingKlineOpenTimes(startOpenTime, endOpenTime int64, interval KlineInterval) ([]int64, error) {
	var months int
	var milli int64
	if interval == Kline1mo {
		months = 1
	} else {
		var ok bool
		milli, ok = KlineIntervalToMilli[interval]
		if !ok {
			return nil, ErrKlineIntervalNotSupported
		}
	}

	missingTs := []int64{}

	for start := time.UnixMilli(startOpenTime).Add(time.Duration(milli)).AddDate(0, months, 0); start.Before(time.UnixMilli(endOpenTime)); start = start.Add(time.Duration(milli)).AddDate(0, months, 0) {
		missingTs = append(missingTs, start.UnixMilli())
	}

	return missingTs, nil
}

type KlineVerifyResult struct {
	Interval      KlineInterval
	InvalidKlines []bnc.Kline
	MissingTs     []int64
	OK            bool
}

func VerifyKlines(klines []bnc.Kline, maxCpus int) (KlineVerifyResult, error) {
	result := KlineVerifyResult{}

	if len(klines) == 0 {
		return KlineVerifyResult{}, errors.New("empty klines")
	}

	first := klines[0]

	interval, err := CalKlineInterval(first)
	if err != nil {
		return result, err
	}
	result.Interval = interval

	if maxCpus <= 0 {
		maxCpus = 1
	}

	groups := props.DivideIntoGroups(klines, len(klines)/maxCpus)

	for i, group := range groups[:len(groups)-1] {
		if groups[i+1] == nil {
			continue
		}
		nextOpenTime := group[len(group)-1].CloseTime + 1
		nextGroupOpenTime := groups[i+1][0].OpenTime
		if nextOpenTime == nextGroupOpenTime {
			continue
		}
		missingTs, err := CalMissingKlineOpenTimes(nextOpenTime, nextGroupOpenTime, interval)
		if err != nil {
			return result, err
		}
		result.MissingTs = append(result.MissingTs, missingTs...)
	}

	wg := errgroup.Group{}
	wg.SetLimit(maxCpus)
	mu := sync.Mutex{}

	for _, group := range groups {
		group := group
		if len(group) == 0 {
			continue
		}
		wg.Go(func() error {
			// TODO should check the kline is valid
			for i, kline := range group[:len(group)-1] {
				nextOpenTime := group[i+1].OpenTime
				missingTs, err := CalMissingKlineOpenTimes(kline.OpenTime, nextOpenTime, interval)
				if err != nil {
					return err
				}
				mu.Lock()
				result.MissingTs = append(result.MissingTs, missingTs...)
				mu.Unlock()
			}
			return nil
		})
	}

	if err := wg.Wait(); err != nil {
		return result, err
	}

	if len(result.MissingTs) == 0 && len(result.InvalidKlines) == 0 {
		result.OK = true
	}

	return result, nil
}
