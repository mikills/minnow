package kb

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mongoManifestDoc is the BSON document schema for manifest storage.
type mongoManifestDoc struct {
	ID        string                `json:"_id" bson:"_id"`
	Version   string                `json:"version" bson:"version"`
	Manifest  SnapshotShardManifest `json:"manifest" bson:"manifest"`
	UpdatedAt time.Time             `json:"updated_at" bson:"updated_at"`
}

// MongoManifestStore implements ManifestStore backed by a MongoDB collection.
// The caller owns the mongo.Client lifecycle.
type MongoManifestStore struct {
	Collection *mongo.Collection

	clockMu sync.RWMutex
	clock   Clock
}

// NewMongoManifestStore creates a MongoManifestStore from a *mongo.Collection.
func NewMongoManifestStore(collection *mongo.Collection) *MongoManifestStore {
	return &MongoManifestStore{Collection: collection, clock: RealClock}
}

// SetClock replaces the store's Clock. Safe for concurrent use.
func (s *MongoManifestStore) SetClock(c Clock) {
	s.clockMu.Lock()
	defer s.clockMu.Unlock()
	if c == nil {
		s.clock = RealClock
		return
	}
	s.clock = c
}

func (s *MongoManifestStore) now() time.Time {
	s.clockMu.RLock()
	defer s.clockMu.RUnlock()
	return nowFrom(s.clock)
}

func (s *MongoManifestStore) collection() (*mongo.Collection, error) {
	if s == nil || s.Collection == nil {
		return nil, ErrInvalidManifestStore
	}
	return s.Collection, nil
}

func (s *MongoManifestStore) Get(ctx context.Context, kbID string) (*ManifestDocument, error) {
	collection, err := s.collection()
	if err != nil {
		return nil, err
	}

	var doc mongoManifestDoc
	err = collection.FindOne(ctx, bson.M{"_id": kbID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrManifestNotFound
		}
		return nil, err
	}

	return &ManifestDocument{
		Manifest: doc.Manifest,
		Version:  doc.Version,
	}, nil
}

func (s *MongoManifestStore) HeadVersion(ctx context.Context, kbID string) (string, error) {
	collection, err := s.collection()
	if err != nil {
		return "", err
	}

	var doc struct {
		Version string `bson:"version" json:"version"`
	}

	err = collection.FindOne(
		ctx, bson.M{"_id": kbID},
		options.FindOne().SetProjection(bson.M{"version": 1}),
	).Decode(&doc)

	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", nil
		}

		return "", err
	}
	return doc.Version, nil
}

func (s *MongoManifestStore) UpsertIfMatch(ctx context.Context, kbID string, manifest SnapshotShardManifest, expectedVersion string) (string, error) {
	collection, err := s.collection()
	if err != nil {
		return "", err
	}

	newVersion := uuid.New().String()
	doc := mongoManifestDoc{
		ID:        kbID,
		Version:   newVersion,
		Manifest:  manifest,
		UpdatedAt: s.now(),
	}

	if expectedVersion == "" {
		// Insert-only: reject if a document already exists for this kbID.
		_, err := collection.InsertOne(ctx, doc)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return "", ErrBlobVersionMismatch
			}

			return "", err
		}
		return newVersion, nil
	}

	// CAS: only replace if current version matches.
	res, err := collection.ReplaceOne(ctx,
		bson.M{"_id": kbID, "version": expectedVersion},
		doc,
	)

	if err != nil {
		return "", err
	}

	if res.MatchedCount == 0 {
		return "", ErrBlobVersionMismatch
	}

	return newVersion, nil
}

func (s *MongoManifestStore) Delete(ctx context.Context, kbID string) error {
	collection, err := s.collection()
	if err != nil {
		return err
	}

	_, err = collection.DeleteOne(ctx, bson.M{"_id": kbID})
	return err
}
