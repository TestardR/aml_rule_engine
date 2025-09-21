// Fan-out/Fan-in Pattern:
// Streaming data: Continuous flow of data
// Pipeline processing: Multiple transformation stages
// Memory efficiency: Don't want to hold all data in memory
// Functional approach: Clean separation of concerns
// Scalable processing: Can handle varying data sizes

package main

import (
	"context"
	"sort"
	"sync"

	"github.com/google/uuid"
)

type UserJob struct {
	UserID       uuid.UUID
	Transactions []Transaction
}

type UserResult struct {
	UserID       uuid.UUID
	HasViolation bool
}

type ConcurrentVelocityProcessor struct {
	Periods     []VelocityPeriod
	WorkerCount int
}

func NewConcurrentVelocityProcessor(periods []VelocityPeriod, workerCount int) ConcurrentVelocityProcessor {
	return ConcurrentVelocityProcessor{
		Periods:     periods,
		WorkerCount: workerCount,
	}
}

func (v ConcurrentVelocityProcessor) Process(ctx context.Context, transactions []Transaction) map[uuid.UUID]struct{} {
	userJobs := v.fanOut(ctx, transactions)

	results := v.process(ctx, userJobs)

	return v.fanIn(results)
}

func (v ConcurrentVelocityProcessor) fanOut(ctx context.Context, transactions []Transaction) <-chan UserJob {
	userJobs := make(chan UserJob, 1000)

	go func() {
		defer close(userJobs)
		userTransactions := make(map[uuid.UUID][]Transaction)

		for _, tx := range transactions {
			userTransactions[tx.UserID] = append(userTransactions[tx.UserID], tx)
		}

		for userID, txs := range userTransactions {
			select {
			case userJobs <- UserJob{UserID: userID, Transactions: txs}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return userJobs
}

func (v ConcurrentVelocityProcessor) process(ctx context.Context, jobs <-chan UserJob) <-chan UserResult {
	results := make(chan UserResult, 1000)
	var wg sync.WaitGroup

	for i := 0; i < v.WorkerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for job := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
					result := v.processUser(job.UserID, job.Transactions)
					results <- result
				}
			}

		}()

	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}

func (v ConcurrentVelocityProcessor) fanIn(results <-chan UserResult) map[uuid.UUID]struct{} {
	flaggedUsers := make(map[uuid.UUID]struct{})
	for result := range results {
		if result.HasViolation {
			flaggedUsers[result.UserID] = struct{}{}
		}
	}

	return flaggedUsers
}

func (v ConcurrentVelocityProcessor) processUser(userID uuid.UUID, txs []Transaction) UserResult {
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

func (v ConcurrentVelocityProcessor) hasViolatedVelocityPeriods(txs []Transaction) bool {
	for _, period := range v.Periods {
		if v.hasViolatedVelocity(txs, period) {
			return true
		}
	}
	return false
}

func (v ConcurrentVelocityProcessor) hasViolatedVelocity(txs []Transaction, period VelocityPeriod) bool {
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
