package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func BenchmarkWorkerVelocityProcessor_Process(b *testing.B) {
	processor := NewWorkerVelocityProcessor([]VelocityPeriod{
		NewVelocityPeriod(week, 5),
		NewVelocityPeriod(month, 20),
		NewVelocityPeriod(year, 100),
	}, 4) // Use 4 workers

	// Create test data
	userCount := 1000
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

func BenchmarkWorkerVelocityProcessor_Process_DifferentWorkerCounts(b *testing.B) {
	periods := []VelocityPeriod{
		NewVelocityPeriod(week, 5),
		NewVelocityPeriod(month, 20),
		NewVelocityPeriod(year, 100),
	}

	// Create test data
	userCount := 1000
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

	workerCounts := []int{1, 2, 4, 8}
	for _, workerCount := range workerCounts {
		b.Run(fmt.Sprintf("Workers_%d", workerCount), func(b *testing.B) {
			processor := NewWorkerVelocityProcessor(periods, workerCount)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				processor.Process(context.Background(), transactions)
			}
		})
	}
}
