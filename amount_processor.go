package main

import (
	"context"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type TransactionAmountProcessor struct {
	Threshold decimal.Decimal
}

func (c TransactionAmountProcessor) Process(_ context.Context, transactions []Transaction) map[uuid.UUID]struct{} {
	flaggedUsers := make(map[uuid.UUID]struct{})

	for _, tx := range transactions {
		if tx.Amount.GreaterThan(c.Threshold) {
			flaggedUsers[tx.UserID] = struct{}{}
		}
	}

	return flaggedUsers
}
