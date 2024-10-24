package bncvision

import (
	"fmt"

	"github.com/dwdwow/cex/bnc"
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
