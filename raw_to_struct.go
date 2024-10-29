package bncvision

import (
	"errors"
	"strconv"

	"github.com/dwdwow/cex/bnc"
)

type RawToStructFunc[T any] func(raw []string) (T, error)

func SpotTradeRawToStruct(raw []string) (bnc.SpotTrade, error) {
	trade := bnc.SpotTrade{}
	var err error
	trade.Id, err = strconv.ParseInt(raw[0], 10, 64)
	if err != nil {
		return trade, err
	}
	trade.Price, err = strconv.ParseFloat(raw[1], 64)
	if err != nil {
		return trade, err
	}
	trade.Qty, err = strconv.ParseFloat(raw[2], 64)
	if err != nil {
		return trade, err
	}
	trade.QuoteQty, err = strconv.ParseFloat(raw[3], 64)
	if err != nil {
		return trade, err
	}
	trade.Time, err = strconv.ParseInt(raw[4], 10, 64)
	if err != nil {
		return trade, err
	}
	trade.IsBuyerMaker, err = strconv.ParseBool(raw[5])
	if err != nil {
		return trade, err
	}
	trade.IsBestMatch, err = strconv.ParseBool(raw[6])
	if err != nil {
		return trade, err
	}
	return trade, nil
}

func AggTradeRawToStruct(raw []string) (bnc.AggTrades, error) {
	trade := bnc.AggTrades{}
	var err error
	trade.Id, err = strconv.ParseInt(raw[0], 10, 64)
	if err != nil {
		return trade, err
	}
	trade.Price, err = strconv.ParseFloat(raw[1], 64)
	if err != nil {
		return trade, err
	}
	trade.Qty, err = strconv.ParseFloat(raw[2], 64)
	if err != nil {
		return trade, err
	}
	trade.FirstTradeId, err = strconv.ParseInt(raw[3], 10, 64)
	if err != nil {
		return trade, err
	}
	trade.LastTradeId, err = strconv.ParseInt(raw[4], 10, 64)
	if err != nil {
		return trade, err
	}
	trade.Time, err = strconv.ParseInt(raw[5], 10, 64)
	if err != nil {
		return trade, err
	}
	trade.IsBuyerMaker, err = strconv.ParseBool(raw[6])
	if err != nil {
		return trade, err
	}
	if len(raw) > 7 {
		trade.IsBestMatch, err = strconv.ParseBool(raw[7])
		if err != nil {
			return trade, err
		}
	}
	return trade, nil
}

func FundingRateRawToStruct(raw []string) (bnc.FuturesFundingRateHistory, error) {
	if len(raw) < 3 {
		return bnc.FuturesFundingRateHistory{}, errors.New("invalid funding rate csv raw")
	}
	fundingRate := bnc.FuturesFundingRateHistory{}
	var err error
	fundingRate.FundingTime, err = strconv.ParseInt(raw[0], 10, 64)
	if err != nil {
		return fundingRate, err
	}
	fundingRate.FundingRate, err = strconv.ParseFloat(raw[2], 64)
	if err != nil {
		return fundingRate, err
	}
	return fundingRate, nil
}

func KlineRawToStruct(raw []string) (bnc.Kline, error) {
	if len(raw) < 12 {
		return bnc.Kline{}, errors.New("invalid kline csv raw")
	}
	kline := bnc.Kline{}
	var err error
	kline.OpenTime, err = strconv.ParseInt(raw[0], 10, 64)
	if err != nil {
		return kline, err
	}
	kline.OpenPrice, err = strconv.ParseFloat(raw[1], 64)
	if err != nil {
		return kline, err
	}
	kline.HighPrice, err = strconv.ParseFloat(raw[2], 64)
	if err != nil {
		return kline, err
	}
	kline.LowPrice, err = strconv.ParseFloat(raw[3], 64)
	if err != nil {
		return kline, err
	}
	kline.ClosePrice, err = strconv.ParseFloat(raw[4], 64)
	if err != nil {
		return kline, err
	}
	kline.Volume, err = strconv.ParseFloat(raw[5], 64)
	if err != nil {
		return kline, err
	}
	kline.CloseTime, err = strconv.ParseInt(raw[6], 10, 64)
	if err != nil {
		return kline, err
	}
	kline.QuoteAssetVolume, err = strconv.ParseFloat(raw[7], 64)
	if err != nil {
		return kline, err
	}
	kline.TradesNumber, err = strconv.ParseInt(raw[8], 10, 64)
	if err != nil {
		return kline, err
	}
	kline.TakerBuyBaseAssetVolume, err = strconv.ParseFloat(raw[9], 64)
	if err != nil {
		return kline, err
	}
	kline.TakerBuyQuoteAssetVolume, err = strconv.ParseFloat(raw[10], 64)
	if err != nil {
		return kline, err
	}
	return kline, nil
}
