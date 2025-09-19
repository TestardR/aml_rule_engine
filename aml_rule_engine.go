package main

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type RuleProcessor interface {
	Process(context.Context, []Transaction) map[uuid.UUID]struct{}
}

type Transaction struct {
	UserID    uuid.UUID
	Amount    decimal.Decimal
	Country   string
	CreatedAt time.Time
}

type RuleEngine struct {
	validators []RuleProcessor
}

func NewRuleEngine(validators []RuleProcessor) *RuleEngine {
	return &RuleEngine{validators: make([]RuleProcessor, 0)}
}

func (r *RuleEngine) AddRuleProcessor(processor RuleProcessor) {
	r.validators = append(r.validators, processor)
}

type CountryBlackListValidator struct {
	Blacklist map[string]struct{}
}

func (c CountryBlackListValidator) Process(_ context.Context, transactions []Transaction) map[uuid.UUID]struct{} {
	flaggedUsers := make(map[uuid.UUID]struct{})

	for _, tx := range transactions {
		if _, exists := c.Blacklist[tx.Country]; exists {
			flaggedUsers[tx.UserID] = struct{}{}
		}
	}

	return flaggedUsers
}

type TransactionAmountValidator struct {
	Threshold decimal.Decimal
}

func (c TransactionAmountValidator) Process(_ context.Context, transactions []Transaction) map[uuid.UUID]struct{} {
	flaggedUsers := make(map[uuid.UUID]struct{})

	for _, tx := range transactions {
		if tx.Amount.GreaterThan(c.Threshold) {
			flaggedUsers[tx.UserID] = struct{}{}
		}
	}

	return flaggedUsers
}

type VelocityValidator struct {
	Threshold int
	Window    time.Duration
}

func (c VelocityValidator) Process(_ context.Context, transactions []Transaction) map[uuid.UUID]struct{} {
	if len(transactions) == 0 {
		return make(map[uuid.UUID]struct{})
	}

	userTransactions := make(map[uuid.UUID][]Transaction)
	for _, tx := range transactions {
		userTransactions[tx.UserID] = append(userTransactions[tx.UserID], tx)
	}

	flaggedUsers := make(map[uuid.UUID]struct{})

	for userID, txs := range userTransactions {
		sort.Slice(txs, func(i, j int) bool {
			return txs[i].CreatedAt.Before(txs[j].CreatedAt)
		})

		if c.checkVelocityWithSlidingWindow(txs) {
			flaggedUsers[userID] = struct{}{}
		}
	}

	return flaggedUsers
}

// Sliding window - Time complexity: O(n) where n is the number of transactions for a user.
func (c VelocityValidator) checkVelocityWithSlidingWindow(txs []Transaction) bool {
	left := 0

	for right := 0; right < len(txs); right++ {
		for left <= right && txs[right].CreatedAt.Sub(txs[left].CreatedAt) > c.Window {
			left++
		}

		windowSize := right - left + 1

		if windowSize > c.Threshold {
			return true
		}
	}

	return false
}
