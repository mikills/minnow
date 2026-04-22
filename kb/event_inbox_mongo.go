// Mongo-backed EventInbox. A unique index on (worker_id, idempotency_key)
// enforces at-most-once side-effect semantics on retried events.

package kb

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoInboxDoc struct {
	ID             string    `bson:"_id"` // worker_id|idempotency_key
	WorkerID       string    `bson:"worker_id"`
	IdempotencyKey string    `bson:"idempotency_key"`
	EventID        string    `bson:"event_id"`
	ProcessedAt    time.Time `bson:"processed_at"`
}

// MongoEventInbox is the Mongo-backed EventInbox.
type MongoEventInbox struct {
	Collection *mongo.Collection

	clockMu sync.RWMutex
	clock   Clock
}

// NewMongoEventInbox constructs the inbox and ensures the dedup + cleanup
// indexes exist. The unique compound index on (worker_id, idempotency_key)
// is what guarantees at-most-once MarkProcessed; relying on the _id default
// would fall apart if the key shape were ever refactored.
func NewMongoEventInbox(ctx context.Context, coll *mongo.Collection) (*MongoEventInbox, error) {
	if coll == nil {
		return nil, errors.New("mongo event inbox: nil collection")
	}
	s := &MongoEventInbox{Collection: coll, clock: RealClock}
	_, err := s.Collection.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "worker_id", Value: 1},
				{Key: "idempotency_key", Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName("uniq_worker_idem"),
		},
		{
			Keys:    bson.D{{Key: "processed_at", Value: 1}},
			Options: options.Index().SetName("processed_at"),
		},
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// SetClock replaces the inbox's Clock. Safe for concurrent use.
func (s *MongoEventInbox) SetClock(c Clock) {
	s.clockMu.Lock()
	defer s.clockMu.Unlock()
	if c == nil {
		s.clock = RealClock
		return
	}
	s.clock = c
}

func (s *MongoEventInbox) now() time.Time {
	s.clockMu.RLock()
	defer s.clockMu.RUnlock()
	return nowFrom(s.clock)
}

// Processed reports whether a row already exists for this worker/key.
func (s *MongoEventInbox) Processed(ctx context.Context, workerID, idempotencyKey string) (bool, error) {
	if idempotencyKey == "" {
		return false, errors.New("inbox: idempotency key required")
	}
	err := s.Collection.FindOne(ctx, bson.M{"_id": workerID + "|" + idempotencyKey}).Err()
	if err == nil {
		return true, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	return false, err
}

// MarkProcessed inserts a row; a duplicate insert signals the event was
// already fully processed by this worker.
func (s *MongoEventInbox) MarkProcessed(ctx context.Context, workerID, idempotencyKey, eventID string) error {
	if idempotencyKey == "" {
		return errors.New("inbox: idempotency key required")
	}
	_, err := s.Collection.InsertOne(ctx, mongoInboxDoc{
		ID:             workerID + "|" + idempotencyKey,
		WorkerID:       workerID,
		IdempotencyKey: idempotencyKey,
		EventID:        eventID,
		ProcessedAt:    s.now(),
	})
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return &ErrInboxDuplicate{IdempotencyKey: idempotencyKey}
		}
		return err
	}
	return nil
}

// Cleanup deletes rows older than cutoff.
func (s *MongoEventInbox) Cleanup(ctx context.Context, olderThan time.Time) (int, error) {
	if olderThan.IsZero() {
		return 0, nil
	}
	res, err := s.Collection.DeleteMany(ctx, bson.M{"processed_at": bson.M{"$lt": olderThan}})
	if err != nil {
		return 0, err
	}
	return int(res.DeletedCount), nil
}
