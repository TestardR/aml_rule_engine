package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func BenchmarkConcurrentVelocityProcessor_Process(b *testing.B) {
	processor := NewConcurrentVelocityProcessor([]VelocityPeriod{
		NewVelocityPeriod(week, 5),
		NewVelocityPeriod(month, 20),
		NewVelocityPeriod(year, 100),
	}, 5)

	// Create test data
	userCount := 10000
	transactionsPerUser := 50
	transactions := make([]Transaction, 0, userCount*transactionsPerUser)

	baseTime := time.Now()
	for i := 0; i < userCount; i++ {
		userID := uuid.New()
		for j := 0; j < transactionsPerUser; j++ {
			transactions = append(transactions, Transaction{
				UserID:    userID,
				Amount:    decimal.NewFromFloat(float64(j * 100)),
				CreatedAt: baseTime.Add(time.Duration(j) * time.Hour),
			})
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processor.Process(context.Background(), transactions)
	}
}
