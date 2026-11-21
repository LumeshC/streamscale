package store

// ProcessOutcome is the result of an idempotent event write.
type ProcessOutcome string

const (
	OutcomeInserted  ProcessOutcome = "inserted"
	OutcomeDuplicate ProcessOutcome = "duplicate"
)
