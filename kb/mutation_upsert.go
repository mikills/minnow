package kb

import "context"

// DeleteDocsOptions controls the behavior of DeleteDocs.
type DeleteDocsOptions struct {
	HardDelete   bool
	CleanupGraph bool
}

// UpsertDocsOptions controls per-call upsert behavior.
type UpsertDocsOptions struct {
	// GraphEnabled overrides graph extraction for this upsert call when set.
	// nil keeps default behavior (graph extraction follows configured GraphBuilder).
	GraphEnabled *bool
}

// UpsertDocs inserts or updates documents in the KB.
func (l *KB) UpsertDocs(ctx context.Context, kbID string, docs []Document) error {
	if err := l.ValidateDocumentReferences(ctx, kbID, docs); err != nil {
		return err
	}
	format, err := l.resolveFormat(ctx, kbID)
	if err != nil {
		return err
	}
	_, err = format.Ingest(ctx, IngestUpsertRequest{KBID: kbID, Docs: docs, Upload: false, Options: UpsertDocsOptions{}})
	return err
}

// UpsertDocsAndUpload uploads after upsert without retry.
func (l *KB) UpsertDocsAndUpload(ctx context.Context, kbID string, docs []Document) error {
	return l.UpsertDocsAndUploadWithRetryAndOptions(ctx, kbID, docs, 0, UpsertDocsOptions{})
}

// UpsertDocsAndUploadWithOptions uploads after upsert without retry.
func (l *KB) UpsertDocsAndUploadWithOptions(ctx context.Context, kbID string, docs []Document, opts UpsertDocsOptions) error {
	return l.UpsertDocsAndUploadWithRetryAndOptions(ctx, kbID, docs, 0, opts)
}

// UpsertDocsAndUploadWithRetry upserts docs and uploads with retry logic.
func (l *KB) UpsertDocsAndUploadWithRetry(ctx context.Context, kbID string, docs []Document, maxRetries int) error {
	return l.UpsertDocsAndUploadWithRetryAndOptions(ctx, kbID, docs, maxRetries, UpsertDocsOptions{})
}

// UpsertDocsAndUploadWithRetryAndOptions upserts docs and uploads with retry logic.
func (l *KB) UpsertDocsAndUploadWithRetryAndOptions(ctx context.Context, kbID string, docs []Document, maxRetries int, opts UpsertDocsOptions) error {
	return runWithUploadRetry(ctx, "upsert_docs_upload", maxRetries, l.RetryObserver, func() error {
		format, err := l.resolveFormat(ctx, kbID)
		if err != nil {
			return err
		}

		_, err = format.Ingest(ctx, IngestUpsertRequest{KBID: kbID, Docs: docs, Upload: true, Options: opts})
		return err
	})
}

func (l *KB) PublishPreparedDocs(ctx context.Context, kbID string, docs []EmbeddedDocument, graphResult *GraphBuildResult, opts UpsertDocsOptions) error {
	format, err := l.resolveFormat(ctx, kbID)
	if err != nil {
		return err
	}
	publisher, ok := format.(PreparedArtifactPublisher)
	if !ok {
		return ErrArtifactFormatNotConfigured
	}
	_, err = publisher.PublishPrepared(ctx, PreparedPublishRequest{
		KBID:        kbID,
		Docs:        docs,
		GraphResult: graphResult,
		Upload:      true,
		Options:     opts,
	})
	return err
}

func shouldActivateSharding(policy ShardingPolicy, snapshotBytes int64, vectorRows int64) bool {
	resolved := NormalizeShardingPolicy(policy)
	if snapshotBytes >= resolved.ShardTriggerBytes {
		return true
	}

	if vectorRows >= int64(resolved.ShardTriggerVectorRows) {
		return true
	}

	return false
}

func shardingActivationReason(policy ShardingPolicy, snapshotBytes int64, vectorRows int64) string {
	resolved := NormalizeShardingPolicy(policy)
	switch {
	case snapshotBytes >= resolved.ShardTriggerBytes && vectorRows >= int64(resolved.ShardTriggerVectorRows):
		return "bytes_and_vector_rows"
	case snapshotBytes >= resolved.ShardTriggerBytes:
		return "snapshot_bytes"
	case vectorRows >= int64(resolved.ShardTriggerVectorRows):
		return "vector_rows"
	default:
		return "within_threshold"
	}
}
