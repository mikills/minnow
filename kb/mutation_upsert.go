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
	return l.publishPreparedDocs(ctx, PreparedPublishRequest{KBID: kbID, Docs: docs, GraphResult: graphResult, Options: opts, Upload: true})
}

func (l *KB) publishPreparedDocs(ctx context.Context, req PreparedPublishRequest) error {
	format, err := l.resolveFormat(ctx, req.KBID)
	if err != nil {
		return err
	}
	publisher, ok := format.(PreparedArtifactPublisher)
	if !ok {
		return ErrArtifactFormatNotConfigured
	}
	_, err = publisher.PublishPrepared(ctx, req)
	return err
}

func (l *KB) commitPreparedDocs(ctx context.Context, kbID string) error {
	format, err := l.resolveFormat(ctx, kbID)
	if err != nil {
		return err
	}
	committer, ok := format.(PreparedArtifactCommitter)
	if !ok {
		return ErrArtifactFormatNotConfigured
	}
	return committer.CommitPrepared(ctx, kbID)
}

func (l *KB) publishPreparedStream(ctx context.Context, req PreparedStreamRequest) error {
	format, err := l.resolveFormat(ctx, req.KBID)
	if err != nil {
		return err
	}
	streamer, ok := format.(PreparedArtifactStreamer)
	if !ok {
		return ErrArtifactFormatNotConfigured
	}
	_, err = streamer.PublishPreparedStream(ctx, req)
	return err
}
