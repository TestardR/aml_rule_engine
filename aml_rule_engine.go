package main

import (
	"context"
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
	processors []RuleProcessor
}

func NewRuleEngine(validators []RuleProcessor) *RuleEngine {
	return &RuleEngine{processors: make([]RuleProcessor, 0)}
}

func (r *RuleEngine) AddRuleProcessor(processor RuleProcessor) {
	r.processors = append(r.processors, processor)
}
