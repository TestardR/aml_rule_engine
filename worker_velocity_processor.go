// Worker Pool Pattern:
// Fixed workload size: Known number of jobs upfront
// Simple coordination: Need straightforward worker management
// Memory is not a concern: Can hold all jobs in memory
// Batch processing: Process all items together

package main

import (
	"context"
	"sort"
	"sync"

	"github.com/google/uuid"
)

// WorkerVelocityProcessor uses a worker pool pattern for concurrent processing
type WorkerVelocityProcessor struct {
	Periods     []VelocityPeriod
	WorkerCount int
}

// NewWorkerVelocityProcessor creates a new worker pool processor
func NewWorkerVelocityProcessor(periods []VelocityPeriod, workerCount int) WorkerVelocityProcessor {
	if workerCount <= 0 {
		workerCount = 4 // Default to 4 workers
	}
	return WorkerVelocityProcessor{
		Periods:     periods,
		WorkerCount: workerCount,
	}
}

// Process processes transactions using a worker pool pattern
func (v WorkerVelocityProcessor) Process(ctx context.Context, transactions []Transaction) map[uuid.UUID]struct{} {
	// Step 1: Group transactions by user (sequential - O(N))
	userTransactions := make(map[uuid.UUID][]Transaction)
	for _, tx := range transactions {
		userTransactions[tx.UserID] = append(userTransactions[tx.UserID], tx)
	}

	// Step 2: Create channels for worker communication
	userJobs := make(chan UserJob, len(userTransactions))
	results := make(chan UserResult, len(userTransactions))

	// Step 3: Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < v.WorkerCount; i++ {
		wg.Add(1)
		go v.worker(ctx, &wg, userJobs, results)
	}

	// Step 4: Send jobs to workers
	go func() {
		defer close(userJobs)
		for userID, txs := range userTransactions {
			select {
			case userJobs <- UserJob{UserID: userID, Transactions: txs}:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Step 5: Collect results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Step 6: Aggregate results
	flaggedUsers := make(map[uuid.UUID]struct{})
	for result := range results {
		if result.HasViolation {
			flaggedUsers[result.UserID] = struct{}{}
		}
	}

	return flaggedUsers
}

// worker processes user jobs concurrently
func (v WorkerVelocityProcessor) worker(ctx context.Context, wg *sync.WaitGroup, jobs <-chan UserJob, results chan<- UserResult) {
	defer wg.Done()

	for job := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
			// Process the user's transactions
			result := v.processUser(job.UserID, job.Transactions)

			select {
			case results <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

// processUser processes a single user's transactions (the expensive part)
func (v WorkerVelocityProcessor) processUser(userID uuid.UUID, txs []Transaction) UserResult {
	// Sort transactions (O(T log T))
	sort.Slice(txs, func(i, j int) bool {
		return txs[i].CreatedAt.Before(txs[j].CreatedAt)
	})

	// Check velocity violations (O(P × T))
	hasViolation := v.hasViolatedVelocityPeriods(txs)

	return UserResult{
		UserID:       userID,
		HasViolation: hasViolation,
	}
}

// hasViolatedVelocityPeriods checks if any of the configured periods have velocity violations
func (v WorkerVelocityProcessor) hasViolatedVelocityPeriods(txs []Transaction) bool {
	for _, period := range v.Periods {
		if v.hasViolatedVelocity(txs, period) {
			return true
		}
	}
	return false
}

// hasViolatedVelocity uses sliding window to check if a specific period has velocity violations
// Time complexity: O(n) where n is the number of transactions for a user
func (v WorkerVelocityProcessor) hasViolatedVelocity(txs []Transaction, period VelocityPeriod) bool {
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
