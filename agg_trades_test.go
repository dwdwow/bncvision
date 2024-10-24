package bncvision

import (
	"testing"
	"time"

	"github.com/dwdwow/cex/bnc"
	"github.com/stretchr/testify/assert"
)

func TestAggTradesToKlines(t *testing.T) {
	testCases := []struct {
		name           string
		aggTrades      []bnc.SpotAggTrades
		interval       time.Duration
		expectedKlines []*bnc.Kline
		expectedError  error
	}{
		{
			name:           "Empty aggTrades",
			aggTrades:      []bnc.SpotAggTrades{},
			interval:       time.Minute,
			expectedKlines: nil,
			expectedError:  nil,
		},
		{
			name: "Single aggTrade",
			aggTrades: []bnc.SpotAggTrades{
				{Time: 1609459200000, Price: 100, Qty: 1, IsBuyerMaker: false},
			},
			interval: time.Minute,
			expectedKlines: []*bnc.Kline{
				{
					OpenTime:                 1609459200000,
					CloseTime:                1609459259999,
					OpenPrice:                100,
					ClosePrice:               100,
					HighPrice:                100,
					LowPrice:                 100,
					Volume:                   1,
					QuoteAssetVolume:         100,
					TakerBuyBaseAssetVolume:  1,
					TakerBuyQuoteAssetVolume: 100,
				},
			},
			expectedError: nil,
		},
		{
			name: "Single aggTrade, 1 second after minute",
			aggTrades: []bnc.SpotAggTrades{
				{Time: 1609459201011, Price: 100, Qty: 1, IsBuyerMaker: false},
			},
			interval: time.Minute,
			expectedKlines: []*bnc.Kline{
				{
					OpenTime:                 1609459200000,
					CloseTime:                1609459259999,
					OpenPrice:                100,
					ClosePrice:               100,
					HighPrice:                100,
					LowPrice:                 100,
					Volume:                   1,
					QuoteAssetVolume:         100,
					TakerBuyBaseAssetVolume:  1,
					TakerBuyQuoteAssetVolume: 100,
				},
			},
			expectedError: nil,
		},
		{
			name: "Multiple aggTrades within same kline",
			aggTrades: []bnc.SpotAggTrades{
				{Time: 1609459200000, Price: 100, Qty: 1, IsBuyerMaker: false},
				{Time: 1609459230000, Price: 101, Qty: 2, IsBuyerMaker: true},
				{Time: 1609459250000, Price: 99, Qty: 3, IsBuyerMaker: false},
			},
			interval: time.Minute,
			expectedKlines: []*bnc.Kline{
				{
					OpenTime:                 1609459200000,
					CloseTime:                1609459259999,
					OpenPrice:                100,
					ClosePrice:               99,
					HighPrice:                101,
					LowPrice:                 99,
					Volume:                   6,
					QuoteAssetVolume:         599,
					TakerBuyBaseAssetVolume:  4,
					TakerBuyQuoteAssetVolume: 397,
				},
			},
			expectedError: nil,
		},
		{
			name: "Multiple aggTrades within same kline, 1 second after minute",
			aggTrades: []bnc.SpotAggTrades{
				{Time: 1609459201000, Price: 100, Qty: 1, IsBuyerMaker: false},
				{Time: 1609459231000, Price: 101, Qty: 2, IsBuyerMaker: true},
				{Time: 1609459251000, Price: 99, Qty: 3, IsBuyerMaker: false},
			},
			interval: time.Minute,
			expectedKlines: []*bnc.Kline{
				{
					OpenTime:                 1609459200000,
					CloseTime:                1609459259999,
					OpenPrice:                100,
					ClosePrice:               99,
					HighPrice:                101,
					LowPrice:                 99,
					Volume:                   6,
					QuoteAssetVolume:         599,
					TakerBuyBaseAssetVolume:  4,
					TakerBuyQuoteAssetVolume: 397,
				},
			},
			expectedError: nil,
		},
		{
			name: "Multiple aggTrades across klines",
			aggTrades: []bnc.SpotAggTrades{
				{Time: 1609459200000, Price: 100, Qty: 1, IsBuyerMaker: false},
				{Time: 1609459260000, Price: 101, Qty: 2, IsBuyerMaker: true},
				{Time: 1609459320000, Price: 99, Qty: 3, IsBuyerMaker: false},
			},
			interval: time.Minute,
			expectedKlines: []*bnc.Kline{
				{
					OpenTime:                 1609459200000,
					CloseTime:                1609459259999,
					OpenPrice:                100,
					ClosePrice:               100,
					HighPrice:                100,
					LowPrice:                 100,
					Volume:                   1,
					QuoteAssetVolume:         100,
					TakerBuyBaseAssetVolume:  1,
					TakerBuyQuoteAssetVolume: 100,
				},
				{
					OpenTime:                 1609459260000,
					CloseTime:                1609459319999,
					OpenPrice:                101,
					ClosePrice:               101,
					HighPrice:                101,
					LowPrice:                 101,
					Volume:                   2,
					QuoteAssetVolume:         202,
					TakerBuyBaseAssetVolume:  0,
					TakerBuyQuoteAssetVolume: 0,
				},
				{
					OpenTime:                 1609459320000,
					CloseTime:                1609459379999,
					OpenPrice:                99,
					ClosePrice:               99,
					HighPrice:                99,
					LowPrice:                 99,
					Volume:                   3,
					QuoteAssetVolume:         297,
					TakerBuyBaseAssetVolume:  3,
					TakerBuyQuoteAssetVolume: 297,
				},
			},
			expectedError: nil,
		},
		{
			name: "Multiple aggTrades across klines, 1 second after minute",
			aggTrades: []bnc.SpotAggTrades{
				{Time: 1609459201000, Price: 100, Qty: 1, IsBuyerMaker: false},
				{Time: 1609459261000, Price: 101, Qty: 2, IsBuyerMaker: true},
				{Time: 1609459321000, Price: 99, Qty: 3, IsBuyerMaker: false},
			},
			interval: time.Minute,
			expectedKlines: []*bnc.Kline{
				{
					OpenTime:                 1609459200000,
					CloseTime:                1609459259999,
					OpenPrice:                100,
					ClosePrice:               100,
					HighPrice:                100,
					LowPrice:                 100,
					Volume:                   1,
					QuoteAssetVolume:         100,
					TakerBuyBaseAssetVolume:  1,
					TakerBuyQuoteAssetVolume: 100,
				},
				{
					OpenTime:                 1609459260000,
					CloseTime:                1609459319999,
					OpenPrice:                101,
					ClosePrice:               101,
					HighPrice:                101,
					LowPrice:                 101,
					Volume:                   2,
					QuoteAssetVolume:         202,
					TakerBuyBaseAssetVolume:  0,
					TakerBuyQuoteAssetVolume: 0,
				},
				{
					OpenTime:                 1609459320000,
					CloseTime:                1609459379999,
					OpenPrice:                99,
					ClosePrice:               99,
					HighPrice:                99,
					LowPrice:                 99,
					Volume:                   3,
					QuoteAssetVolume:         297,
					TakerBuyBaseAssetVolume:  3,
					TakerBuyQuoteAssetVolume: 297,
				},
			},
			expectedError: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			klines, err := AggTradesToKlines(tc.aggTrades, tc.interval)

			assert.Equal(t, tc.expectedError, err)
			assert.Equal(t, tc.expectedKlines, klines)
		})
	}
}
