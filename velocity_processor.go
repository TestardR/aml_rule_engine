package main

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"
)

const year = 365 * 24 * time.Hour
const month = 30 * 24 * time.Hour
const week = 7 * 24 * time.Hour

type VelocityProcessor struct {
	Periods []VelocityPeriod
}

// NewVelocityValidator creates a new VelocityProcessor with common time periods
func NewVelocityValidator(periods []VelocityPeriod) VelocityProcessor {
	return VelocityProcessor{
		Periods: periods,
	}
}

type VelocityPeriod struct {
	Duration  time.Duration
	Threshold int
}

func NewVelocityPeriod(period time.Duration, threshold int) VelocityPeriod {
	return VelocityPeriod{
		Duration:  period,
		Threshold: threshold,
	}
}

func (v VelocityProcessor) Process(_ context.Context, transactions []Transaction) map[uuid.UUID]struct{} {
	userTransactions := make(map[uuid.UUID][]Transaction)
	for _, tx := range transactions {
		userTransactions[tx.UserID] = append(userTransactions[tx.UserID], tx)
	}

	flaggedUsers := make(map[uuid.UUID]struct{})

	// O(U * T log T)
	for userID, txs := range userTransactions { // O(U)
		sort.Slice(txs, func(i, j int) bool { // O(T log T)
			return txs[i].CreatedAt.Before(txs[j].CreatedAt)
		})

		if v.hasViolatedVelocityPeriods(txs) { // O(P * T)
			flaggedUsers[userID] = struct{}{}
		}
	}

	return flaggedUsers
}

// hasViolatedVelocityPeriods checks if any of the configured periods have velocity violations
func (v VelocityProcessor) hasViolatedVelocityPeriods(txs []Transaction) bool {
	for _, period := range v.Periods {
		if v.hasViolatedVelocity(txs, period) {
			return true
		}
	}

	return false
}

// hasViolatedVelocity uses sliding window to check if a specific period has velocity violations
// Time complexity: O(n) where n is the number of transactions for a user
func (v VelocityProcessor) hasViolatedVelocity(txs []Transaction, period VelocityPeriod) bool {
	left := 0

	for right := 0; right < len(txs); right++ {
		for left <= right && txs[right].CreatedAt.Sub(txs[left].CreatedAt) > period.Duration {
			left++
		}

		windowSize := right - left + 1

		if windowSize > period.Threshold {
			return true
		}
	}

	return false
}
