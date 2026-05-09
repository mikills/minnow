package manifest

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mikills/minnow/kb/blobstore"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const mongoKeyID = "_id"

type Clock interface{ Now() time.Time }

type mongoManifestDoc struct {
	ID        string        `json:"_id"        bson:"_id"`
	Version   string        `json:"version"    bson:"version"`
	Manifest  ShardManifest `json:"manifest"   bson:"manifest"`
	UpdatedAt time.Time     `json:"updated_at" bson:"updated_at"`
}

type MongoStore struct {
	Collection *mongo.Collection
	clockMu    sync.RWMutex
	clock      Clock
}

func NewMongoStore(collection *mongo.Collection) *MongoStore {
	return &MongoStore{Collection: collection, clock: realClock{}}
}

func (s *MongoStore) SetClock(c Clock) {
	s.clockMu.Lock()
	defer s.clockMu.Unlock()
	if c == nil {
		s.clock = realClock{}
		return
	}
	s.clock = c
}

func (s *MongoStore) now() time.Time {
	s.clockMu.RLock()
	defer s.clockMu.RUnlock()
	if s.clock == nil {
		return time.Now().UTC()
	}
	return s.clock.Now()
}

func (s *MongoStore) collection() (*mongo.Collection, error) {
	if s == nil || s.Collection == nil {
		return nil, ErrInvalidStore
	}
	return s.Collection, nil
}

func (s *MongoStore) Get(ctx context.Context, kbID string) (*Document, error) {
	collection, err := s.collection()
	if err != nil {
		return nil, err
	}
	var doc mongoManifestDoc
	err = collection.FindOne(ctx, bson.M{mongoKeyID: kbID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &Document{Manifest: doc.Manifest, Version: doc.Version}, nil
}

func (s *MongoStore) HeadVersion(ctx context.Context, kbID string) (string, error) {
	collection, err := s.collection()
	if err != nil {
		return "", err
	}
	var doc struct {
		Version string `bson:"version" json:"version"`
	}
	err = collection.FindOne(ctx, bson.M{mongoKeyID: kbID}, options.FindOne().SetProjection(bson.M{"version": 1})).
		Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", nil
		}
		return "", err
	}
	return doc.Version, nil
}

func (s *MongoStore) UpsertIfMatch(
	ctx context.Context,
	kbID string,
	manifest ShardManifest,
	expectedVersion string,
) (string, error) {
	collection, err := s.collection()
	if err != nil {
		return "", err
	}
	newVersion := uuid.New().String()
	doc := mongoManifestDoc{ID: kbID, Version: newVersion, Manifest: manifest, UpdatedAt: s.now()}
	if expectedVersion == "" {
		_, err := collection.InsertOne(ctx, doc)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return "", blobstore.ErrVersionMismatch
			}
			return "", err
		}
		return newVersion, nil
	}
	res, err := collection.ReplaceOne(ctx, bson.M{mongoKeyID: kbID, "version": expectedVersion}, doc)
	if err != nil {
		return "", err
	}
	if res.MatchedCount == 0 {
		return "", blobstore.ErrVersionMismatch
	}
	return newVersion, nil
}

func (s *MongoStore) Delete(ctx context.Context, kbID string) error {
	collection, err := s.collection()
	if err != nil {
		return err
	}
	_, err = collection.DeleteOne(ctx, bson.M{mongoKeyID: kbID})
	return err
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }
