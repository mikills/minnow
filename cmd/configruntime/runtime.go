// Package configruntime assembles a live minnow deployment from a
// config.Config value: KB, artifact format, HTTP app, scheduler, and
// worker pools. It is the only package that bridges the schema (kb/config)
// to the concrete backends (kb/duckdb, Mongo drivers, local blob store).
package configruntime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"time"

	appcmd "github.com/mikills/minnow/cmd"
	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/config"
	kbduckdb "github.com/mikills/minnow/kb/duckdb"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

// BuildOptions tunes how Build assembles the runtime.
type BuildOptions struct {
	// DryRun: construct every object but do not open network connections,
	// bind ports, mutate the filesystem, or start goroutines.
	DryRun bool
	// Logger is used for informational startup logs. If nil, slog.Default().
	Logger *slog.Logger
}

// Runtime is a fully wired but not-yet-started deployment. Call Start to
// take traffic; call Stop to shut down cleanly.
type Runtime struct {
	cfg         *config.Config
	logger      *slog.Logger
	dryRun      bool
	kb          *kb.KB
	app         *appcmd.App
	scheduler   *kb.Scheduler
	workerPools []*kb.WorkerPool
	cleanups    []func(context.Context) error
}

// KB returns the configured KB instance.
func (r *Runtime) KB() *kb.KB { return r.kb }

// App returns the HTTP app. Nil is possible only before Build returns.
func (r *Runtime) App() *appcmd.App { return r.app }

// Scheduler returns the configured scheduler, or nil if disabled.
func (r *Runtime) Scheduler() *kb.Scheduler { return r.scheduler }

// WorkerPools returns the configured event worker pools.
func (r *Runtime) WorkerPools() []*kb.WorkerPool { return r.workerPools }

// Build constructs the deployment. It does not open the HTTP listener, mutate
// the filesystem, connect to Mongo in dry-run mode, or start any goroutine.
func Build(ctx context.Context, cfg *config.Config, opts BuildOptions) (*Runtime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configruntime: cfg must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	rt := &Runtime{cfg: cfg, logger: logger, dryRun: opts.DryRun}

	blobStore, err := buildBlobStore(cfg)
	if err != nil {
		return nil, err
	}

	embedder, err := buildEmbedder(cfg, logger)
	if err != nil {
		return nil, err
	}

	kbOpts := []kb.KBOption{
		kb.WithEmbedder(embedder),
		kb.WithShardingPolicy(cfg.ShardingPolicy()),
		kb.WithMediaGCConfig(cfg.MediaGCConfig()),
		kb.WithMediaContentTypeAllowlist(cfg.Media.ContentTypeAllowlist),
	}

	if cfg.Graph.Enabled {
		grapher := kb.NewOllamaGrapher(cfg.Graph.URL, cfg.Graph.Model)
		grapher.MaxParallel = cfg.GraphParallelism()
		kbOpts = append(kbOpts, kb.WithGraphBuilder(&kb.GraphBuilder{
			Chunker: &kb.TextChunker{ChunkSize: kb.DefaultTextChunkSize},
			Grapher: grapher,
		}))
		logger.Info("configured ollama grapher", "url", cfg.Graph.URL, "model", cfg.Graph.Model, "parallelism", cfg.GraphParallelism())
	}

	mongoOpts, err := rt.wireMongo(ctx, cfg)
	if err != nil {
		return nil, err
	}
	kbOpts = append(kbOpts, mongoOpts...)

	// Event pipeline: minnow is event-driven everywhere. When Mongo is not
	// configured (or we're in DryRun), wire the in-memory implementations so
	// /rag/ingest, worker pools, and operation polling all work locally.
	// Mongo, when present, has already installed its own EventStore/Inbox
	// via wireMongo - we don't stomp it here.
	if !hasEventStore(mongoOpts) {
		kbOpts = append(kbOpts, kb.WithEventStore(kb.NewInMemoryEventStore()))
	}
	if !hasEventInbox(mongoOpts) {
		kbOpts = append(kbOpts, kb.WithEventInbox(kb.NewInMemoryEventInbox()))
	}

	// Media store: only wire one when cfg.Media.Enabled is true. With media
	// disabled the routes return 503 (cmd/app.go gates the Dependencies
	// closures on KB.MediaStore being non-nil). Mongo media wiring is
	// likewise skipped inside wireMongo when media is disabled.
	if cfg.Media.Enabled && !hasMediaStore(mongoOpts) {
		kbOpts = append(kbOpts, kb.WithMediaStore(kb.NewInMemoryMediaStore()))
	}

	k := kb.NewKB(blobStore, cfg.Storage.Cache.Dir, kbOpts...)
	if cfg.Storage.Cache.MaxBytes > 0 {
		k.SetMaxCacheBytes(cfg.Storage.Cache.MaxBytes)
	}
	if d := cfg.Storage.Cache.EntryTTL.AsDuration(); d > 0 {
		k.SetCacheEntryTTL(d)
	}
	rt.kb = k

	af, err := kbduckdb.NewArtifactFormat(kbduckdb.NewDepsFromKB(k,
		kbduckdb.WithMemoryLimit(cfg.Format.DuckDB.MemoryLimit),
		kbduckdb.WithExtensionDir(cfg.Format.DuckDB.ExtensionDir),
		kbduckdb.WithOfflineExt(cfg.Format.DuckDB.Offline),
	))
	if err != nil {
		return nil, fmt.Errorf("build duckdb artifact format: %w", err)
	}
	if err := k.RegisterFormat(af); err != nil {
		return nil, fmt.Errorf("register artifact format: %w", err)
	}

	appCfg := appcmd.AppConfig{
		Address:               cfg.HTTP.Address,
		ReadHeaderTimeout:     cfg.HTTPReadHeaderTimeout(),
		ShutdownTimeout:       cfg.HTTPShutdownTimeout(),
		CacheEvictionInterval: cfg.CacheEvictInterval(),
		MaxMediaBytes:         cfg.Media.MaxBytes,
		Logger:                logger,
	}
	rt.app = appcmd.NewApp(k, appCfg)

	if cfg.Scheduler.Enabled {
		rt.scheduler = kb.NewScheduler(k.WriteLeaseManager, cfg.SchedulerTick(), cfg.Scheduler.DisabledJobs, nil)
		if err := k.RegisterDefaultJobs(rt.scheduler); err != nil {
			return nil, fmt.Errorf("register scheduler jobs: %w", err)
		}
	}

	if k.EventStore != nil {
		pools, err := buildWorkerPools(k, cfg, rt.app)
		if err != nil {
			return nil, err
		}
		rt.workerPools = pools
	}

	return rt, nil
}

// Start takes the runtime live: ensures filesystem state, starts the
// scheduler, starts worker pools, and binds the HTTP listener. Safe to call
// only once. In DryRun mode, Start returns nil without side effects (no
// mkdir, no port bind, no goroutines).
func (r *Runtime) Start(ctx context.Context) error {
	if r.dryRun {
		return nil
	}
	if err := os.MkdirAll(r.cfg.Storage.Blob.Root, 0o755); err != nil {
		return fmt.Errorf("create blob root %q: %w", r.cfg.Storage.Blob.Root, err)
	}
	if err := os.MkdirAll(r.cfg.Storage.Cache.Dir, 0o755); err != nil {
		return fmt.Errorf("create cache dir %q: %w", r.cfg.Storage.Cache.Dir, err)
	}
	if r.scheduler != nil {
		r.scheduler.Start()
		r.logger.Info("scheduler started", "tick_interval", r.cfg.SchedulerTick(), "jobs", r.scheduler.JobIDs())
	}
	for _, pool := range r.workerPools {
		pool.Start(ctx)
	}
	if err := r.app.Start(); err != nil {
		return fmt.Errorf("start app: %w", err)
	}
	r.logger.Info("minnow listening", "address", r.app.Address())
	return nil
}

// Wait blocks until the HTTP app exits (on ctx cancellation or error).
// In DryRun mode, returns immediately.
func (r *Runtime) Wait() error {
	if r.dryRun || r.app == nil {
		return nil
	}
	return r.app.Wait()
}

// Stop performs graceful shutdown in reverse startup order: worker pools,
// scheduler, HTTP app, then any connection cleanups (e.g. Mongo disconnect).
// Safe to call multiple times.
func (r *Runtime) Stop(ctx context.Context) error {
	for _, pool := range r.workerPools {
		pool.Stop()
	}
	if r.scheduler != nil {
		r.scheduler.Stop()
	}
	if !r.dryRun && r.app != nil {
		if err := r.app.Stop(ctx); err != nil {
			r.logger.Error("shutdown error", "error", err)
		}
	}
	for _, fn := range r.cleanups {
		if err := fn(ctx); err != nil {
			r.logger.Error("runtime cleanup", "error", err)
		}
	}
	return nil
}

func buildBlobStore(cfg *config.Config) (kb.BlobStore, error) {
	switch cfg.Storage.Blob.Kind {
	case "local":
		return &kb.LocalBlobStore{Root: cfg.Storage.Blob.Root}, nil
	default:
		return nil, fmt.Errorf("configruntime: blob kind %q not supported", cfg.Storage.Blob.Kind)
	}
}

func buildEmbedder(cfg *config.Config, logger *slog.Logger) (kb.Embedder, error) {
	switch cfg.Embedder.Provider {
	case "ollama":
		logger.Info("configured ollama embedder", "url", cfg.Embedder.Ollama.URL, "model", cfg.Embedder.Ollama.Model)
		return kb.NewOllamaEmbedder(cfg.Embedder.Ollama.URL, cfg.Embedder.Ollama.Model), nil
	case "local":
		e, err := kb.NewLocalEmbedder(cfg.Embedder.Local.Dim)
		if err != nil {
			return nil, fmt.Errorf("build local embedder: %w", err)
		}
		logger.Info("configured local embedder", "dim", cfg.Embedder.Local.Dim)
		return e, nil
	default:
		return nil, fmt.Errorf("configruntime: embedder provider %q not supported", cfg.Embedder.Provider)
	}
}

// wireMongo connects to Mongo and returns the KBOption slice that wires
// the manifest store, event store, inbox, and (when media is enabled) the
// media store. In DryRun mode or when cfg.Mongo is nil, returns no options.
func (r *Runtime) wireMongo(ctx context.Context, cfg *config.Config) ([]kb.KBOption, error) {
	if cfg.Mongo == nil || r.dryRun {
		return nil, nil
	}

	// The mongo driver may echo the connection URI (and therefore any
	// embedded credentials) in its error text. Log the underlying error
	// against a redacted URI internally, and return a fixed message to the
	// caller: never let the raw driver error escape to logs or API responses
	// that might include the secret.
	redactedURI := redactMongoURI(cfg.Mongo.URI)
	client, err := mongo.Connect(mongoopts.Client().ApplyURI(cfg.Mongo.URI))
	if err != nil {
		r.logger.Error("mongo connect failed", "uri", redactedURI, "error", err.Error())
		return nil, errors.New("mongo connect failed: check MINNOW_MONGO_URI")
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		r.logger.Error("mongo ping failed", "uri", redactedURI, "error", err.Error())
		return nil, errors.New("mongo ping failed: check MINNOW_MONGO_URI and network reachability")
	}

	r.cleanups = append(r.cleanups, func(ctx context.Context) error {
		return client.Disconnect(ctx)
	})

	db := client.Database(cfg.Mongo.Database)
	initCtx, cancelInit := context.WithTimeout(ctx, 10*time.Second)
	defer cancelInit()

	eventStore, err := kb.NewMongoEventStore(initCtx, db.Collection(cfg.Mongo.Collections.Events), client)
	if err != nil {
		return nil, fmt.Errorf("mongo event store: %w", err)
	}
	inbox, err := kb.NewMongoEventInbox(initCtx, db.Collection(cfg.Mongo.Collections.Inbox))
	if err != nil {
		return nil, fmt.Errorf("mongo event inbox: %w", err)
	}

	out := []kb.KBOption{
		kb.WithManifestStore(kb.NewMongoManifestStore(db.Collection(cfg.Mongo.Collections.Manifests))),
		kb.WithEventStore(eventStore),
		kb.WithEventInbox(inbox),
	}

	if cfg.Media.Enabled {
		mediaStore, err := kb.NewMongoMediaStore(initCtx, db.Collection(cfg.Mongo.Collections.Media))
		if err != nil {
			return nil, fmt.Errorf("mongo media store: %w", err)
		}
		out = append(out, kb.WithMediaStore(mediaStore))
	}

	r.logger.Info("configured mongo stores",
		"db", cfg.Mongo.Database,
		"manifests", cfg.Mongo.Collections.Manifests,
		"events", cfg.Mongo.Collections.Events,
		"inbox", cfg.Mongo.Collections.Inbox,
		"media", cfg.Mongo.Collections.Media,
		"media_enabled", cfg.Media.Enabled,
	)

	return out, nil
}

// optionsApply runs every KBOption against a scratch KB so we can introspect
// which fields the slice would set, so the local fallback doesn't clobber a
// store that Mongo wiring already provided.
func optionsApply(opts []kb.KBOption) *kb.KB {
	scratch := &kb.KB{}
	for _, opt := range opts {
		if opt != nil {
			opt(scratch)
		}
	}
	return scratch
}

func hasMediaStore(opts []kb.KBOption) bool {
	if len(opts) == 0 {
		return false
	}
	return optionsApply(opts).MediaStore != nil
}

func hasEventStore(opts []kb.KBOption) bool {
	if len(opts) == 0 {
		return false
	}
	return optionsApply(opts).EventStore != nil
}

func hasEventInbox(opts []kb.KBOption) bool {
	if len(opts) == 0 {
		return false
	}
	return optionsApply(opts).EventInbox != nil
}

func buildWorkerPools(k *kb.KB, cfg *config.Config, app *appcmd.App) ([]*kb.WorkerPool, error) {
	type entry struct {
		worker kb.Worker
		pool   config.WorkerPool
	}
	// Document workers are unconditional; media-upload is only constructed
	// when media is enabled. Constructing the worker without a wired
	// MediaStore would let it claim media.upload events from the queue and
	// fail every Handle call with "media subsystem not configured", driving
	// retries and dead-letters for events the operator explicitly disabled.
	entries := []entry{
		{&kb.DocumentUpsertWorker{KB: k, ID: "document-upsert-worker"}, cfg.Workers.DocumentUpsert},
		{&kb.DocumentChunkedWorker{KB: k, ID: "document-chunked-worker"}, cfg.Workers.DocumentChunked},
		{&kb.DocumentPublishWorker{KB: k, ID: "document-publish-embedded-worker", KindValue: kb.EventDocumentEmbedded}, cfg.Workers.DocumentPublish},
		{&kb.DocumentPublishWorker{KB: k, ID: "document-publish-graph-worker", KindValue: kb.EventDocumentGraphExtracted}, cfg.Workers.DocumentPublish},
	}
	if cfg.Media.Enabled {
		entries = append(entries, entry{&kb.MediaUploadWorker{KB: k, ID: "media-upload-worker"}, cfg.Workers.MediaUpload})
	}

	pools := make([]*kb.WorkerPool, 0, len(entries))
	for _, e := range entries {
		poolCfg := cfg.PoolConfigFor(e.pool)
		pool, err := kb.NewWorkerPool(e.worker, k.EventStore, k.EventInbox, poolCfg)
		if err != nil {
			return nil, fmt.Errorf("build worker pool for %T: %w", e.worker, err)
		}
		if m := app.Metrics(); m != nil {
			pool.SetMetrics(workerMetricsAdapter{m})
		}
		pools = append(pools, pool)
	}
	return pools, nil
}

// redactMongoURI strips credentials (user:password@) from a mongo URI so it
// can be safely included in logs. Non-URI input is returned unchanged except
// for a fixed sentinel so operators can tell redaction ran.
func redactMongoURI(uri string) string {
	if uri == "" {
		return ""
	}
	u, err := url.Parse(uri)
	if err != nil || u.Host == "" {
		return "<unparseable-uri>"
	}
	u.User = nil
	return u.String()
}

// workerMetricsAdapter bridges kb.WorkerMetrics to appcmd.AppMetrics.
type workerMetricsAdapter struct{ m kb.AppMetrics }

func (a workerMetricsAdapter) OnWorkerTick(kind kb.EventKind, workerID, outcome string, duration time.Duration, _ error) {
	a.m.RecordWorkerTick(string(kind), workerID, outcome, duration.Milliseconds())
}
