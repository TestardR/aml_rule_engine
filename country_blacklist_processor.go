package main

import (
	"context"

	"github.com/google/uuid"
)

type CountryBlackListProcessor struct {
	Blacklist map[string]struct{}
}

func (c CountryBlackListProcessor) Process(_ context.Context, transactions []Transaction) map[uuid.UUID]struct{} {
	flaggedUsers := make(map[uuid.UUID]struct{})

	for _, tx := range transactions {
		if _, exists := c.Blacklist[tx.Country]; exists {
			flaggedUsers[tx.UserID] = struct{}{}
		}
	}

	return flaggedUsers
}
