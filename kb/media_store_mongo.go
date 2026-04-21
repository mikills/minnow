package kb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Pagination tuneables for the media store.
const (
	MediaPageDefaultLimit = 500
	MediaPageMaxLimit     = 5000
)

// MediaPage is a single page of results returned by paginated media queries.
// NextToken is empty when no more pages exist.
type MediaPage struct {
	Items     []MediaObject
	NextToken string
}

type mediaPageCursor struct {
	LastID string `json:"last_id"`
}

func encodeMediaPageToken(lastID string) string {
	if lastID == "" {
		return ""
	}
	b, err := json.Marshal(mediaPageCursor{LastID: lastID})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeMediaPageToken(token string) (string, error) {
	if token == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", errors.New("media: invalid page token")
	}
	var c mediaPageCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return "", errors.New("media: invalid page token")
	}
	return c.LastID, nil
}

func clampMediaLimit(limit int) int {
	if limit <= 0 {
		return MediaPageDefaultLimit
	}
	if limit > MediaPageMaxLimit {
		return MediaPageMaxLimit
	}
	return limit
}

// MongoMediaStore persists MediaObject metadata in a Mongo collection.
type MongoMediaStore struct {
	Collection *mongo.Collection
}

// NewMongoMediaStore constructs the store and ensures helpful indexes exist.
func NewMongoMediaStore(ctx context.Context, coll *mongo.Collection) (*MongoMediaStore, error) {
	if coll == nil {
		return nil, errors.New("mongo media store: nil collection")
	}
	s := &MongoMediaStore{Collection: coll}
	_, err := s.Collection.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "kb_id", Value: 1}, {Key: "state", Value: 1}}, Options: options.Index().SetName("kb_state")},
		{Keys: bson.D{{Key: "kb_id", Value: 1}, {Key: "filename", Value: 1}}, Options: options.Index().SetName("kb_filename")},
		{Keys: bson.D{{Key: "kb_id", Value: 1}, {Key: "idempotency_key", Value: 1}}, Options: options.Index().SetName("kb_idempotency").SetUnique(true).SetPartialFilterExpression(bson.D{{Key: "idempotency_key", Value: bson.D{{Key: "$gt", Value: ""}}}})},
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

type mongoMediaDoc struct {
	ID              string         `bson:"_id"`
	KBID            string         `bson:"kb_id"`
	Filename        string         `bson:"filename"`
	ContentType     string         `bson:"content_type"`
	SizeBytes       int64          `bson:"size_bytes"`
	BlobKey         string         `bson:"blob_key"`
	Checksum        string         `bson:"checksum"`
	Source          string         `bson:"source,omitempty"`
	Title           string         `bson:"title,omitempty"`
	Tags            []string       `bson:"tags,omitempty"`
	CreatedAtUnixMs int64          `bson:"created_at_unix_ms"`
	UploadedBy      string         `bson:"uploaded_by,omitempty"`
	Metadata        map[string]any `bson:"metadata,omitempty"`
	State           string         `bson:"state"`
	TombstonedAtMs  int64          `bson:"tombstoned_at_unix_ms,omitempty"`
	IdempotencyKey  string         `bson:"idempotency_key,omitempty"`
}

func toMongoMedia(m MediaObject) mongoMediaDoc {
	return mongoMediaDoc{
		ID: m.ID, KBID: m.KBID, Filename: m.Filename, ContentType: m.ContentType,
		SizeBytes: m.SizeBytes, BlobKey: m.BlobKey, Checksum: m.Checksum,
		Source: m.Source, Title: m.Title, Tags: m.Tags,
		CreatedAtUnixMs: m.CreatedAtUnixMs, UploadedBy: m.UploadedBy, Metadata: m.Metadata,
		State: string(m.State), TombstonedAtMs: m.TombstonedAtMs, IdempotencyKey: m.IdempotencyKey,
	}
}

func fromMongoMedia(d mongoMediaDoc) MediaObject {
	return MediaObject{
		ID: d.ID, KBID: d.KBID, Filename: d.Filename, ContentType: d.ContentType,
		SizeBytes: d.SizeBytes, BlobKey: d.BlobKey, Checksum: d.Checksum,
		Source: d.Source, Title: d.Title, Tags: d.Tags,
		CreatedAtUnixMs: d.CreatedAtUnixMs, UploadedBy: d.UploadedBy, Metadata: d.Metadata,
		State: MediaState(d.State), TombstonedAtMs: d.TombstonedAtMs, IdempotencyKey: d.IdempotencyKey,
	}
}

func (s *MongoMediaStore) Put(ctx context.Context, m MediaObject) error {
	if m.ID == "" {
		return errors.New("media: id required")
	}
	_, err := s.Collection.ReplaceOne(ctx, bson.M{"_id": m.ID}, toMongoMedia(m),
		options.Replace().SetUpsert(true))
	if err != nil && mongo.IsDuplicateKeyError(err) {
		return ErrMediaDuplicateKey
	}
	return err
}

func (s *MongoMediaStore) Get(ctx context.Context, mediaID string) (*MediaObject, error) {
	var doc mongoMediaDoc
	if err := s.Collection.FindOne(ctx, bson.M{"_id": mediaID}).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrMediaNotFound
		}
		return nil, err
	}
	out := fromMongoMedia(doc)
	return &out, nil
}

func (s *MongoMediaStore) FindByIdempotency(ctx context.Context, kbID, idempotencyKey string) (*MediaObject, error) {
	if idempotencyKey == "" {
		return nil, nil
	}
	var doc mongoMediaDoc
	err := s.Collection.FindOne(ctx, bson.M{"kb_id": kbID, "idempotency_key": idempotencyKey}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	out := fromMongoMedia(doc)
	return &out, nil
}

// List returns a single page of media under kbID, optionally filtered by a
// filename prefix. The prefix uses an indexed range query (kb_filename
// compound index) rather than a regex anchor that would force a collection
// scan and open a ReDoS surface.
func (s *MongoMediaStore) List(ctx context.Context, kbID, prefix string, after string, limit int) (MediaPage, error) {
	filter := bson.M{"kb_id": kbID}
	if prefix != "" {
		filter["filename"] = bson.M{"$gte": prefix, "$lt": prefix + "\uffff"}
	}
	return s.findPage(ctx, filter, limit, after)
}

func (s *MongoMediaStore) UpdateState(ctx context.Context, mediaID string, state MediaState, tombstonedAtMs int64) error {
	update := bson.M{"$set": bson.M{"state": string(state), "tombstoned_at_unix_ms": tombstonedAtMs}}
	res, err := s.Collection.UpdateOne(ctx, bson.M{"_id": mediaID}, update)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return ErrMediaNotFound
	}
	return nil
}

// ListByState returns a single page of media with the given state. Pass
// after="" for the first page and iterate while the returned NextToken is
// non-empty. Use this in GC / admin loops to bound memory.
func (s *MongoMediaStore) ListByState(ctx context.Context, kbID string, state MediaState, after string, limit int) (MediaPage, error) {
	filter := bson.M{"state": string(state)}
	if kbID != "" {
		filter["kb_id"] = kbID
	}
	return s.findPage(ctx, filter, limit, after)
}

// findPage issues a single keyset-paginated query. Results are sorted by
// _id ascending; the continuation token is the last _id on the page.
func (s *MongoMediaStore) findPage(ctx context.Context, filter bson.M, limit int, pageToken string) (MediaPage, error) {
	lastID, err := decodeMediaPageToken(pageToken)
	if err != nil {
		return MediaPage{}, err
	}
	limit = clampMediaLimit(limit)
	// Copy the caller's filter so we can narrow by _id without mutating it.
	merged := bson.M{}
	for k, v := range filter {
		merged[k] = v
	}
	if lastID != "" {
		merged["_id"] = bson.M{"$gt": lastID}
	}
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(limit))
	cur, err := s.Collection.Find(ctx, merged, opts)
	if err != nil {
		return MediaPage{}, err
	}
	defer cur.Close(ctx)
	out := make([]MediaObject, 0, limit)
	for cur.Next(ctx) {
		var d mongoMediaDoc
		if err := cur.Decode(&d); err != nil {
			return MediaPage{}, err
		}
		out = append(out, fromMongoMedia(d))
	}
	if err := cur.Err(); err != nil {
		return MediaPage{}, err
	}
	page := MediaPage{Items: out}
	if len(out) == limit {
		page.NextToken = encodeMediaPageToken(out[len(out)-1].ID)
	}
	return page, nil
}

func (s *MongoMediaStore) Delete(ctx context.Context, mediaID string) error {
	_, err := s.Collection.DeleteOne(ctx, bson.M{"_id": mediaID})
	return err
}
