package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestVelocityProcessor_Process(t *testing.T) {
	baseTime := time.Now()
	userID1 := uuid.New()
	userID2 := uuid.New()

	tests := []struct {
		name         string
		periods      []VelocityPeriod
		transactions []Transaction
		wantCount    int
		wantUsers    []uuid.UUID
	}{
		{
			name: "no violations",
			periods: []VelocityPeriod{
				NewVelocityPeriod(week, 5),
				NewVelocityPeriod(month, 20),
			},
			transactions: []Transaction{
				{UserID: userID1, Amount: decimal.NewFromFloat(100), CreatedAt: baseTime},
				{UserID: userID1, Amount: decimal.NewFromFloat(200), CreatedAt: baseTime.Add(2 * time.Hour)},
				{UserID: userID2, Amount: decimal.NewFromFloat(150), CreatedAt: baseTime.Add(4 * time.Hour)},
			},
			wantCount: 0,
			wantUsers: []uuid.UUID{},
		},
		{
			name: "weekly violation",
			periods: []VelocityPeriod{
				NewVelocityPeriod(week, 3),
				NewVelocityPeriod(month, 10),
			},
			transactions: []Transaction{
				{UserID: userID1, Amount: decimal.NewFromFloat(100), CreatedAt: baseTime},
				{UserID: userID1, Amount: decimal.NewFromFloat(200), CreatedAt: baseTime.Add(1 * time.Hour)},
				{UserID: userID1, Amount: decimal.NewFromFloat(300), CreatedAt: baseTime.Add(2 * time.Hour)},
				{UserID: userID1, Amount: decimal.NewFromFloat(400), CreatedAt: baseTime.Add(3 * time.Hour)},
			},
			wantCount: 1,
			wantUsers: []uuid.UUID{userID1},
		},
		{
			name: "monthly violation",
			periods: []VelocityPeriod{
				NewVelocityPeriod(week, 10),
				NewVelocityPeriod(month, 3),
			},
			transactions: []Transaction{
				{UserID: userID1, Amount: decimal.NewFromFloat(100), CreatedAt: baseTime},
				{UserID: userID1, Amount: decimal.NewFromFloat(200), CreatedAt: baseTime.Add(7 * 24 * time.Hour)},
				{UserID: userID1, Amount: decimal.NewFromFloat(300), CreatedAt: baseTime.Add(14 * 24 * time.Hour)},
				{UserID: userID1, Amount: decimal.NewFromFloat(400), CreatedAt: baseTime.Add(21 * 24 * time.Hour)},
			},
			wantCount: 1,
			wantUsers: []uuid.UUID{userID1},
		},
		{
			name: "multiple users - one violation",
			periods: []VelocityPeriod{
				NewVelocityPeriod(week, 2),
			},
			transactions: []Transaction{
				// User 1: 3 transactions (violation)
				{UserID: userID1, Amount: decimal.NewFromFloat(100), CreatedAt: baseTime},
				{UserID: userID1, Amount: decimal.NewFromFloat(200), CreatedAt: baseTime.Add(1 * time.Hour)},
				{UserID: userID1, Amount: decimal.NewFromFloat(300), CreatedAt: baseTime.Add(2 * time.Hour)},
				// User 2: 1 transaction (no violation)
				{UserID: userID2, Amount: decimal.NewFromFloat(150), CreatedAt: baseTime.Add(3 * time.Hour)},
			},
			wantCount: 1,
			wantUsers: []uuid.UUID{userID1},
		},
		{
			name: "exact threshold - no violation",
			periods: []VelocityPeriod{
				NewVelocityPeriod(week, 3),
			},
			transactions: []Transaction{
				{UserID: userID1, Amount: decimal.NewFromFloat(100), CreatedAt: baseTime},
				{UserID: userID1, Amount: decimal.NewFromFloat(200), CreatedAt: baseTime.Add(1 * time.Hour)},
				{UserID: userID1, Amount: decimal.NewFromFloat(300), CreatedAt: baseTime.Add(2 * time.Hour)},
			},
			wantCount: 0,
			wantUsers: []uuid.UUID{},
		},
		{
			name: "unsorted transactions",
			periods: []VelocityPeriod{
				NewVelocityPeriod(week, 3),
			},
			transactions: []Transaction{
				{UserID: userID1, Amount: decimal.NewFromFloat(400), CreatedAt: baseTime.Add(3 * time.Hour)},
				{UserID: userID1, Amount: decimal.NewFromFloat(100), CreatedAt: baseTime},
				{UserID: userID1, Amount: decimal.NewFromFloat(300), CreatedAt: baseTime.Add(2 * time.Hour)},
				{UserID: userID1, Amount: decimal.NewFromFloat(200), CreatedAt: baseTime.Add(1 * time.Hour)},
			},
			wantCount: 1,
			wantUsers: []uuid.UUID{userID1},
		},
		{
			name: "transactions outside period",
			periods: []VelocityPeriod{
				NewVelocityPeriod(week, 2),
			},
			transactions: []Transaction{
				{UserID: userID1, Amount: decimal.NewFromFloat(100), CreatedAt: baseTime},
				{UserID: userID1, Amount: decimal.NewFromFloat(200), CreatedAt: baseTime.Add(1 * time.Hour)},
				{UserID: userID1, Amount: decimal.NewFromFloat(300), CreatedAt: baseTime.Add(8 * 24 * time.Hour)}, // 8 days later
			},
			wantCount: 0,
			wantUsers: []uuid.UUID{},
		},
		{
			name:         "empty transactions",
			periods:      []VelocityPeriod{NewVelocityPeriod(week, 3)},
			transactions: []Transaction{},
			wantCount:    0,
			wantUsers:    []uuid.UUID{},
		},
		{
			name:    "no periods configured",
			periods: []VelocityPeriod{},
			transactions: []Transaction{
				{UserID: userID1, Amount: decimal.NewFromFloat(100), CreatedAt: baseTime},
				{UserID: userID1, Amount: decimal.NewFromFloat(200), CreatedAt: baseTime.Add(1 * time.Hour)},
				{UserID: userID1, Amount: decimal.NewFromFloat(300), CreatedAt: baseTime.Add(2 * time.Hour)},
			},
			wantCount: 0,
			wantUsers: []uuid.UUID{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := NewVelocityValidator(tt.periods)
			flaggedUsers := processor.Process(context.Background(), tt.transactions)

			assert.Equal(t, tt.wantCount, len(flaggedUsers), "Expected %d flagged users, got %d", tt.wantCount, len(flaggedUsers))

			// Check specific users
			for _, wantUser := range tt.wantUsers {
				assert.Contains(t, flaggedUsers, wantUser, "Expected user %s to be flagged", wantUser)
			}

			// Check that no unexpected users are flagged
			for userID := range flaggedUsers {
				assert.Contains(t, tt.wantUsers, userID, "Unexpected user %s was flagged", userID)
			}
		})
	}
}

// Benchmark tests
func BenchmarkVelocityProcessor_Process(b *testing.B) {
	processor := NewVelocityValidator([]VelocityPeriod{
		NewVelocityPeriod(week, 5),
		NewVelocityPeriod(month, 20),
		NewVelocityPeriod(year, 100),
	})

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
