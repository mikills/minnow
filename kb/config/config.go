// Package config owns the YAML schema for a minnow deployment.
//
// It is a pure schema/loader package: it depends on kb-level types and the
// YAML library, never on kb/duckdb, kb/mongo drivers, or cmd. Runtime
// assembly (wiring KB, scheduler, worker pools, HTTP app) lives in a
// separate package.
package config

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the full YAML-addressable deployment config.
type Config struct {
	HTTP      HTTPConfig      `yaml:"http" json:"http"`
	Storage   StorageConfig   `yaml:"storage" json:"storage"`
	Format    FormatConfig    `yaml:"format" json:"format"`
	Embedder  EmbedderConfig  `yaml:"embedder" json:"embedder"`
	Graph     GraphConfig     `yaml:"graph" json:"graph"`
	Mongo     *MongoConfig    `yaml:"mongo,omitempty" json:"mongo,omitempty"`
	Scheduler SchedulerConfig `yaml:"scheduler" json:"scheduler"`
	Workers   WorkersConfig   `yaml:"workers" json:"workers"`
	Media     MediaConfig     `yaml:"media" json:"media"`
	Sharding  ShardingConfig  `yaml:"sharding" json:"sharding"`
}

// HTTPConfig describes the public HTTP server.
//
// Timeouts are pointer types so the validator can distinguish
// "operator wrote 0" (rejected as out-of-range) from "operator omitted the
// key" (defaulted by HTTPReadHeaderTimeout / HTTPShutdownTimeout helpers).
type HTTPConfig struct {
	Address           string    `yaml:"address" json:"address"`
	ReadHeaderTimeout *Duration `yaml:"read_header_timeout,omitempty" json:"read_header_timeout,omitempty"`
	ShutdownTimeout   *Duration `yaml:"shutdown_timeout,omitempty" json:"shutdown_timeout,omitempty"`
}

// StorageConfig groups blob storage and local cache settings.
type StorageConfig struct {
	Blob  BlobConfig  `yaml:"blob" json:"blob"`
	Cache CacheConfig `yaml:"cache" json:"cache"`
}

// BlobConfig selects a blob backend. Only "local" is implemented today.
type BlobConfig struct {
	Kind string `yaml:"kind" json:"kind"`
	Root string `yaml:"root" json:"root"`
}

// CacheConfig tunes the local shard cache. EntryTTL is non-pointer because
// 0 has a meaning (no TTL); EvictInterval is a pointer so explicit-zero is
// rejected by the validator.
type CacheConfig struct {
	Dir           string    `yaml:"dir" json:"dir"`
	MaxBytes      int64     `yaml:"max_bytes" json:"max_bytes"`
	EntryTTL      Duration  `yaml:"entry_ttl" json:"entry_ttl"`
	EvictInterval *Duration `yaml:"evict_interval,omitempty" json:"evict_interval,omitempty"`
}

// FormatConfig selects the artifact format backend.
type FormatConfig struct {
	Kind   string             `yaml:"kind" json:"kind"`
	DuckDB DuckDBFormatConfig `yaml:"duckdb" json:"duckdb"`
}

// DuckDBFormatConfig carries DuckDB-specific knobs.
type DuckDBFormatConfig struct {
	MemoryLimit  string `yaml:"memory_limit" json:"memory_limit"`
	ExtensionDir string `yaml:"extension_dir" json:"extension_dir"`
	Offline      bool   `yaml:"offline" json:"offline"`
}

// EmbedderConfig selects an embedder implementation.
type EmbedderConfig struct {
	Provider string                `yaml:"provider" json:"provider"`
	Ollama   *OllamaEmbedderConfig `yaml:"ollama,omitempty" json:"ollama,omitempty"`
	Local    *LocalEmbedderConfig  `yaml:"local,omitempty" json:"local,omitempty"`
}

// OllamaEmbedderConfig configures the Ollama provider.
type OllamaEmbedderConfig struct {
	URL   string `yaml:"url" json:"url"`
	Model string `yaml:"model" json:"model"`
}

// LocalEmbedderConfig configures the deterministic local embedder.
type LocalEmbedderConfig struct {
	Dim int `yaml:"dim" json:"dim"`
}

// GraphConfig configures optional graph extraction.
type GraphConfig struct {
	Enabled     bool   `yaml:"enabled" json:"enabled"`
	URL         string `yaml:"url" json:"url"`
	Model       string `yaml:"model" json:"model"`
	Parallelism *int   `yaml:"parallelism,omitempty" json:"parallelism,omitempty"`
}

// MongoConfig configures Mongo-backed stores. If the whole block is omitted,
// the process runs in local/dev mode with in-memory stores.
type MongoConfig struct {
	URI         string                 `yaml:"uri" json:"uri"`
	Database    string                 `yaml:"database" json:"database"`
	Collections MongoCollectionsConfig `yaml:"collections" json:"collections"`
}

// MongoCollectionsConfig names the four collections used by minnow.
type MongoCollectionsConfig struct {
	Manifests string `yaml:"manifests" json:"manifests"`
	Events    string `yaml:"events" json:"events"`
	Inbox     string `yaml:"inbox" json:"inbox"`
	Media     string `yaml:"media" json:"media"`
}

// SchedulerConfig configures the periodic-jobs scheduler.
type SchedulerConfig struct {
	Enabled      bool      `yaml:"enabled" json:"enabled"`
	TickInterval *Duration `yaml:"tick_interval,omitempty" json:"tick_interval,omitempty"`
	DisabledJobs []string  `yaml:"disabled_jobs" json:"disabled_jobs"`
}

// WorkersConfig configures per-worker-pool sizing and timeouts.
type WorkersConfig struct {
	Defaults        WorkerDefaults `yaml:"defaults" json:"defaults"`
	DocumentUpsert  WorkerPool     `yaml:"document_upsert" json:"document_upsert"`
	DocumentChunked WorkerPool     `yaml:"document_chunked" json:"document_chunked"`
	DocumentPublish WorkerPool     `yaml:"document_publish" json:"document_publish"`
	MediaUpload     WorkerPool     `yaml:"media_upload" json:"media_upload"`
}

// WorkerDefaults applies to every pool unless overridden.
type WorkerDefaults struct {
	MaxAttempts       int      `yaml:"max_attempts" json:"max_attempts"`
	PollInterval      Duration `yaml:"poll_interval" json:"poll_interval"`
	VisibilityTimeout Duration `yaml:"visibility_timeout" json:"visibility_timeout"`
}

// WorkerPool describes one worker pool. The timeout/retry overrides are
// optional; when nil the pool inherits WorkerDefaults.
type WorkerPool struct {
	Concurrency       int       `yaml:"concurrency" json:"concurrency"`
	MaxAttempts       *int      `yaml:"max_attempts,omitempty" json:"max_attempts,omitempty"`
	PollInterval      *Duration `yaml:"poll_interval,omitempty" json:"poll_interval,omitempty"`
	VisibilityTimeout *Duration `yaml:"visibility_timeout,omitempty" json:"visibility_timeout,omitempty"`
}

// MediaConfig tunes the media subsystem.
type MediaConfig struct {
	Enabled              bool     `yaml:"enabled" json:"enabled"`
	MaxBytes             int64    `yaml:"max_bytes" json:"max_bytes"`
	ContentTypeAllowlist []string `yaml:"content_type_allowlist" json:"content_type_allowlist"`
	PendingTTL           Duration `yaml:"pending_ttl" json:"pending_ttl"`
	TombstoneGrace       Duration `yaml:"tombstone_grace" json:"tombstone_grace"`
	UploadCompletionTTL  Duration `yaml:"upload_completion_ttl" json:"upload_completion_ttl"`
}

// ShardingConfig mirrors kb.ShardingPolicy. Pointer fields preserve the
// "present in YAML" vs "absent, use default" distinction that
// kb.NormalizeShardingPolicy relies on.
type ShardingConfig struct {
	ShardTriggerBytes           *int64   `yaml:"shard_trigger_bytes,omitempty" json:"shard_trigger_bytes,omitempty"`
	ShardTriggerVectorRows      *int     `yaml:"shard_trigger_vector_rows,omitempty" json:"shard_trigger_vector_rows,omitempty"`
	TargetShardBytes            *int64   `yaml:"target_shard_bytes,omitempty" json:"target_shard_bytes,omitempty"`
	MaxVectorRowsPerShard       *int     `yaml:"max_vector_rows_per_shard,omitempty" json:"max_vector_rows_per_shard,omitempty"`
	QueryShardFanout            *int     `yaml:"query_shard_fanout,omitempty" json:"query_shard_fanout,omitempty"`
	QueryShardFanoutAdaptiveMax *int     `yaml:"query_shard_fanout_adaptive_max,omitempty" json:"query_shard_fanout_adaptive_max,omitempty"`
	QueryShardParallelism       *int     `yaml:"query_shard_parallelism,omitempty" json:"query_shard_parallelism,omitempty"`
	QueryShardLocalTopKMult     *int     `yaml:"query_shard_local_topk_multiplier,omitempty" json:"query_shard_local_topk_multiplier,omitempty"`
	SmallKBMaxShards            *int     `yaml:"small_kb_max_shards,omitempty" json:"small_kb_max_shards,omitempty"`
	CompactionEnabled           *bool    `yaml:"compaction_enabled,omitempty" json:"compaction_enabled,omitempty"`
	CompactionMinShardCount     *int     `yaml:"compaction_min_shard_count,omitempty" json:"compaction_min_shard_count,omitempty"`
	CompactionTombstoneRatio    *float64 `yaml:"compaction_tombstone_ratio,omitempty" json:"compaction_tombstone_ratio,omitempty"`
}

// Duration is a time.Duration that unmarshals from a YAML string like "5s"
// or "1m30s", preserving line-number context on parse failure.
type Duration time.Duration

// AsDuration returns the underlying time.Duration.
func (d Duration) AsDuration() time.Duration { return time.Duration(d) }

// UnmarshalYAML parses a duration string with a line-number-aware error.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("line %d: duration must be a string like \"5s\"", node.Line)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("line %d: invalid duration %q: %w", node.Line, s, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML renders the duration as its canonical string form.
func (d Duration) MarshalYAML() (interface{}, error) {
	return time.Duration(d).String(), nil
}

// applyDefaults fills in defaults for any non-pointer field the YAML left
// unset. Pointer fields (HTTP timeouts, scheduler tick, cache evict
// interval, graph parallelism) are intentionally NOT touched here so the
// validator can distinguish "operator wrote 0 explicitly" from "operator
// omitted the key". Helper accessors below resolve those pointers (or fall
// back to defaults) at call time.
//
// Called by Load after strict decoding, before validation.
func (c *Config) applyDefaults() {
	if c.HTTP.Address == "" {
		c.HTTP.Address = "127.0.0.1:8080"
	}

	if c.Storage.Blob.Kind == "" {
		c.Storage.Blob.Kind = "local"
	}
	if c.Storage.Blob.Root == "" {
		c.Storage.Blob.Root = "./.temp/fixtures"
	}
	if c.Storage.Cache.Dir == "" {
		c.Storage.Cache.Dir = "./.temp/cache"
	}

	if c.Format.Kind == "" {
		c.Format.Kind = "duckdb"
	}
	if c.Format.DuckDB.MemoryLimit == "" {
		c.Format.DuckDB.MemoryLimit = "128MB"
	}
	if c.Format.DuckDB.ExtensionDir == "" {
		c.Format.DuckDB.ExtensionDir = "./extensions"
	}

	if c.Embedder.Provider == "" {
		c.Embedder.Provider = "ollama"
	}
	// Note: we intentionally do NOT auto-create Embedder.Ollama / Embedder.Local
	// here. The validator rejects a selected provider with a missing config
	// block so the operator sees an explicit error instead of silently running
	// with default URL/model/dim. Populate only the interior fields when the
	// block is already present.
	if c.Embedder.Ollama != nil {
		if c.Embedder.Ollama.URL == "" {
			c.Embedder.Ollama.URL = "http://localhost:11434"
		}
		if c.Embedder.Ollama.Model == "" {
			c.Embedder.Ollama.Model = "all-minilm"
		}
	}
	if c.Embedder.Local != nil && c.Embedder.Local.Dim == 0 {
		c.Embedder.Local.Dim = 384
	}

	if c.Graph.Enabled {
		if c.Graph.URL == "" {
			c.Graph.URL = "http://localhost:11434"
		}
		if c.Graph.Model == "" {
			c.Graph.Model = "llama3"
		}
		// Parallelism is a pointer: keep nil so validator catches explicit
		// non-positive values; GraphParallelism() resolves at call time.
	}

	if c.Mongo != nil {
		if c.Mongo.Database == "" {
			c.Mongo.Database = "minnow"
		}
		if c.Mongo.Collections.Manifests == "" {
			c.Mongo.Collections.Manifests = "manifests"
		}
		if c.Mongo.Collections.Events == "" {
			c.Mongo.Collections.Events = "kb_events"
		}
		if c.Mongo.Collections.Inbox == "" {
			c.Mongo.Collections.Inbox = "kb_event_inbox"
		}
		if c.Mongo.Collections.Media == "" {
			c.Mongo.Collections.Media = "media"
		}
	}

	if c.Workers.Defaults.MaxAttempts == 0 {
		c.Workers.Defaults.MaxAttempts = 5
	}
	if c.Workers.Defaults.PollInterval == 0 {
		c.Workers.Defaults.PollInterval = Duration(500 * time.Millisecond)
	}
	if c.Workers.Defaults.VisibilityTimeout == 0 {
		c.Workers.Defaults.VisibilityTimeout = Duration(5 * time.Minute)
	}
	if c.Workers.DocumentUpsert.Concurrency == 0 {
		c.Workers.DocumentUpsert.Concurrency = 4
	}
	if c.Workers.DocumentChunked.Concurrency == 0 {
		c.Workers.DocumentChunked.Concurrency = 4
	}
	if c.Workers.DocumentPublish.Concurrency == 0 {
		c.Workers.DocumentPublish.Concurrency = 2
	}
	if c.Workers.MediaUpload.Concurrency == 0 {
		c.Workers.MediaUpload.Concurrency = 2
	}

	if c.Media.MaxBytes == 0 {
		c.Media.MaxBytes = 10 * 1024 * 1024
	}
	if c.Media.PendingTTL == 0 {
		c.Media.PendingTTL = Duration(24 * time.Hour)
	}
	if c.Media.TombstoneGrace == 0 {
		c.Media.TombstoneGrace = Duration(1 * time.Hour)
	}
	if c.Media.UploadCompletionTTL == 0 {
		c.Media.UploadCompletionTTL = Duration(15 * time.Minute)
	}
}
