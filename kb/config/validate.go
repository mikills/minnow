package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/mikills/minnow/kb"
)

// Validate checks cross-field and per-field invariants that strict YAML
// decoding cannot express. It runs after applyDefaults and path resolution,
// so every field visible here has either an explicit user value or a schema
// default filled in.
//
// Errors are accumulated so operators see the full list of problems in one
// pass rather than fixing them one at a time.
func (c *Config) Validate() error {
	var errs []string

	errs = appendIf(errs, c.validateHTTP())
	errs = appendIf(errs, c.validateStorage())
	errs = appendIf(errs, c.validateFormat())
	errs = appendIf(errs, c.validateEmbedder())
	errs = appendIf(errs, c.validateGraph())
	errs = appendIf(errs, c.validateMongo())
	errs = appendIf(errs, c.validateScheduler())
	errs = appendIf(errs, c.validateWorkers())
	errs = appendIf(errs, c.validateMedia())
	errs = appendIf(errs, c.validateSharding())

	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "; "))
}

func appendIf(errs []string, err error) []string {
	if err != nil {
		errs = append(errs, err.Error())
	}
	return errs
}

func (c *Config) validateHTTP() error {
	if err := requireNonEmptyString("http.address", c.HTTP.Address); err != nil {
		return err
	}
	if err := requirePositiveDuration("http.read_header_timeout", c.HTTP.ReadHeaderTimeout); err != nil {
		return err
	}
	if err := requirePositiveDuration("http.shutdown_timeout", c.HTTP.ShutdownTimeout); err != nil {
		return err
	}
	return nil
}

func (c *Config) validateStorage() error {
	if c.Storage.Blob.Kind != "local" {
		return fmt.Errorf("storage.blob.kind %q is not supported (only \"local\")", c.Storage.Blob.Kind)
	}
	if err := requireNonEmptyString("storage.blob.root", c.Storage.Blob.Root); err != nil {
		return err
	}
	if err := requireNonEmptyString("storage.cache.dir", c.Storage.Cache.Dir); err != nil {
		return err
	}
	if err := requireNonNegativeInt64("storage.cache.max_bytes", c.Storage.Cache.MaxBytes, 0); err != nil {
		return err
	}
	if err := requireNonNegativeDurationValue("storage.cache.entry_ttl", c.Storage.Cache.EntryTTL, "0 disables the TTL"); err != nil {
		return err
	}
	if err := requirePositiveDuration("storage.cache.evict_interval", c.Storage.Cache.EvictInterval); err != nil {
		return err
	}
	return nil
}

func (c *Config) validateGraph() error {
	if !c.Graph.Enabled {
		return nil
	}
	if err := requireEnabledString("graph.enabled", "graph.url", c.Graph.URL); err != nil {
		return err
	}
	parsed, err := url.Parse(c.Graph.URL)
	if err != nil {
		return fmt.Errorf("graph.url must be a valid URL: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("graph.url must include a scheme and host (got %q)", c.Graph.URL)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("graph.url scheme must be http or https (got %q)", parsed.Scheme)
	}
	if err := requireEnabledString("graph.enabled", "graph.model", c.Graph.Model); err != nil {
		return err
	}
	if err := requirePositiveIntPtr("graph.parallelism", c.Graph.Parallelism); err != nil {
		return err
	}
	return nil
}

func (c *Config) validateScheduler() error {
	if c.Scheduler.Enabled && c.Scheduler.TickInterval == nil {
		return errors.New("scheduler.tick_interval must be set when scheduler.enabled is true")
	}
	return requirePositiveDuration("scheduler.tick_interval", c.Scheduler.TickInterval)
}

// requirePositiveDuration enforces "when present, must be > 0" on pointer
// duration fields. nil is allowed (caller falls back to a default via the
// helper accessors in build_inputs.go).
func requirePositiveDuration(name string, d *Duration) error {
	if d == nil {
		return nil
	}
	if d.AsDuration() <= 0 {
		return fmt.Errorf("%s must be > 0 when set, got %s", name, d.AsDuration())
	}
	return nil
}

func (c *Config) validateFormat() error {
	if c.Format.Kind != "duckdb" {
		return fmt.Errorf("format.kind %q is not supported (only \"duckdb\")", c.Format.Kind)
	}
	if err := requireNonEmptyString("format.duckdb.memory_limit", c.Format.DuckDB.MemoryLimit); err != nil {
		return err
	}
	return nil
}

func (c *Config) validateEmbedder() error {
	switch c.Embedder.Provider {
	case "ollama":
		if c.Embedder.Ollama == nil {
			return errors.New("embedder.ollama block is required when embedder.provider is \"ollama\"")
		}
		if err := requireNonEmptyString("embedder.ollama.url", c.Embedder.Ollama.URL); err != nil {
			return err
		}
		if err := requireNonEmptyString("embedder.ollama.model", c.Embedder.Ollama.Model); err != nil {
			return err
		}
	case "local":
		if c.Embedder.Local == nil {
			return errors.New("embedder.local block is required when embedder.provider is \"local\"")
		}
		if err := requirePositiveInt("embedder.local.dim", c.Embedder.Local.Dim); err != nil {
			return err
		}
	default:
		return fmt.Errorf("embedder.provider %q is not supported (expected \"ollama\" or \"local\")", c.Embedder.Provider)
	}
	return nil
}

func (c *Config) validateMongo() error {
	if c.Mongo == nil {
		return nil
	}
	if err := requireNonEmptyString("mongo.uri", c.Mongo.URI); err != nil {
		return errors.New("mongo.uri must be set when mongo block is present (reference a secret via ${VAR})")
	}
	if err := requireNonEmptyString("mongo.database", c.Mongo.Database); err != nil {
		return err
	}
	col := c.Mongo.Collections
	if col.Manifests == "" || col.Events == "" || col.Inbox == "" || col.Media == "" {
		return errors.New("mongo.collections must set all four names: manifests, events, inbox, media")
	}
	// Guard against all four names collapsing to the same collection: that
	// would silently corrupt state at runtime because every store would write
	// into the same Mongo collection.
	named := []struct {
		field string
		value string
	}{
		{"mongo.collections.manifests", col.Manifests},
		{"mongo.collections.events", col.Events},
		{"mongo.collections.inbox", col.Inbox},
		{"mongo.collections.media", col.Media},
	}
	seen := make(map[string]string, len(named))
	for _, n := range named {
		if prev, ok := seen[n.value]; ok {
			return fmt.Errorf("%s and %s must be distinct (both set to %q)", prev, n.field, n.value)
		}
		seen[n.value] = n.field
	}
	return nil
}

func (c *Config) validateWorkers() error {
	if c.Workers.Defaults.MaxAttempts <= 0 {
		return errors.New("workers.defaults.max_attempts must be > 0")
	}
	if c.Workers.Defaults.PollInterval <= 0 {
		return errors.New("workers.defaults.poll_interval must be > 0")
	}
	if c.Workers.Defaults.VisibilityTimeout <= 0 {
		return errors.New("workers.defaults.visibility_timeout must be > 0")
	}
	pools := map[string]WorkerPool{
		"document_upsert":  c.Workers.DocumentUpsert,
		"document_chunked": c.Workers.DocumentChunked,
		"document_publish": c.Workers.DocumentPublish,
		"media_upload":     c.Workers.MediaUpload,
	}
	for name, p := range pools {
		if p.Concurrency <= 0 {
			return fmt.Errorf("workers.%s.concurrency must be > 0", name)
		}
		if p.MaxAttempts != nil && *p.MaxAttempts <= 0 {
			return fmt.Errorf("workers.%s.max_attempts must be > 0 when set", name)
		}
		if p.PollInterval != nil && *p.PollInterval <= 0 {
			return fmt.Errorf("workers.%s.poll_interval must be > 0 when set", name)
		}
		if p.VisibilityTimeout != nil && *p.VisibilityTimeout <= 0 {
			return fmt.Errorf("workers.%s.visibility_timeout must be > 0 when set", name)
		}
	}
	return nil
}

func (c *Config) validateMedia() error {
	if !c.Media.Enabled {
		return nil
	}
	if err := requirePositiveInt64("media.max_bytes", c.Media.MaxBytes); err != nil {
		return err
	}
	if err := requirePositiveDurationValue("media.pending_ttl", c.Media.PendingTTL); err != nil {
		return err
	}
	if err := requirePositiveDurationValue("media.tombstone_grace", c.Media.TombstoneGrace); err != nil {
		return err
	}
	if err := requirePositiveDurationValue("media.upload_completion_ttl", c.Media.UploadCompletionTTL); err != nil {
		return err
	}
	// Tombstones must not outlive the pending window: otherwise a
	// tombstoned media row is still eligible for purge before an unreferenced
	// pending upload is GC'd, leading to confusing operator UX.
	if c.Media.TombstoneGrace > c.Media.PendingTTL {
		return fmt.Errorf("media.tombstone_grace (%s) must be <= media.pending_ttl (%s)",
			c.Media.TombstoneGrace.AsDuration(), c.Media.PendingTTL.AsDuration())
	}
	return nil
}

// validateSharding enforces both per-field positive-int rules and the
// cross-field constraints from main.go's parseShardingPolicyFromEnv. Checks
// run on the user-supplied values where present and on the kb package
// defaults where absent, so a partially-specified block (e.g. user set
// query_shard_fanout but not adaptive max) is still validated end-to-end.
func (c *Config) validateSharding() error {
	resolved := c.Sharding.resolve()

	const maxSafeFanout = 64
	const maxTopKMult = 16

	if err := checkPositive64("shard_trigger_bytes", c.Sharding.ShardTriggerBytes); err != nil {
		return err
	}
	if err := checkPositive("shard_trigger_vector_rows", c.Sharding.ShardTriggerVectorRows); err != nil {
		return err
	}
	if err := checkPositive64("target_shard_bytes", c.Sharding.TargetShardBytes); err != nil {
		return err
	}
	if err := checkPositive("max_vector_rows_per_shard", c.Sharding.MaxVectorRowsPerShard); err != nil {
		return err
	}
	if err := checkPositive("query_shard_fanout", c.Sharding.QueryShardFanout); err != nil {
		return err
	}
	if err := checkPositive("query_shard_fanout_adaptive_max", c.Sharding.QueryShardFanoutAdaptiveMax); err != nil {
		return err
	}
	if err := checkPositive("query_shard_parallelism", c.Sharding.QueryShardParallelism); err != nil {
		return err
	}
	if err := checkPositive("query_shard_local_topk_multiplier", c.Sharding.QueryShardLocalTopKMult); err != nil {
		return err
	}
	if err := checkPositive("small_kb_max_shards", c.Sharding.SmallKBMaxShards); err != nil {
		return err
	}
	if err := checkPositive("compaction_min_shard_count", c.Sharding.CompactionMinShardCount); err != nil {
		return err
	}
	if c.Sharding.CompactionTombstoneRatio != nil {
		r := *c.Sharding.CompactionTombstoneRatio
		if r <= 0 || r > 1 {
			return fmt.Errorf("sharding.compaction_tombstone_ratio must be in (0, 1], got %g", r)
		}
	}

	if resolved.QueryShardFanout > maxSafeFanout {
		return fmt.Errorf("sharding.query_shard_fanout must be <= %d", maxSafeFanout)
	}
	if resolved.QueryShardFanoutAdaptiveMax > maxSafeFanout {
		return fmt.Errorf("sharding.query_shard_fanout_adaptive_max must be <= %d", maxSafeFanout)
	}
	if resolved.QueryShardFanoutAdaptiveMax < resolved.QueryShardFanout {
		fanoutProv := provenance(c.Sharding.QueryShardFanout != nil)
		adaptiveProv := provenance(c.Sharding.QueryShardFanoutAdaptiveMax != nil)
		return fmt.Errorf("sharding: query_shard_fanout_adaptive_max (%d, %s) must be >= query_shard_fanout (%d, %s)",
			resolved.QueryShardFanoutAdaptiveMax, adaptiveProv,
			resolved.QueryShardFanout, fanoutProv)
	}
	if resolved.QueryShardLocalTopKMult > maxTopKMult {
		return fmt.Errorf("sharding.query_shard_local_topk_multiplier must be <= %d", maxTopKMult)
	}
	return nil
}

func provenance(userSet bool) string {
	if userSet {
		return "user-set"
	}
	return "from default"
}

func checkPositive(name string, v *int) error {
	if v == nil {
		return nil
	}
	if *v <= 0 {
		return fmt.Errorf("sharding.%s must be > 0 when set, got %d", name, *v)
	}
	return nil
}

func checkPositive64(name string, v *int64) error {
	if v == nil {
		return nil
	}
	if *v <= 0 {
		return fmt.Errorf("sharding.%s must be > 0 when set, got %d", name, *v)
	}
	return nil
}

func requireNonEmptyString(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s must be set", name)
	}
	return nil
}

func requireEnabledString(flagName, valueName, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s must be set when %s is true", valueName, flagName)
	}
	return nil
}

func requirePositiveInt(name string, value int) error {
	if value <= 0 {
		return fmt.Errorf("%s must be > 0", name)
	}
	return nil
}

func requirePositiveInt64(name string, value int64) error {
	if value <= 0 {
		return fmt.Errorf("%s must be > 0", name)
	}
	return nil
}

func requireNonNegativeInt64(name string, value int64, floor int64) error {
	if value < floor {
		return fmt.Errorf("%s must be >= %d", name, floor)
	}
	return nil
}

func requirePositiveIntPtr(name string, value *int) error {
	if value == nil {
		return nil
	}
	if *value <= 0 {
		return fmt.Errorf("%s must be > 0 when set, got %d", name, *value)
	}
	return nil
}

func requirePositiveDurationValue(name string, value Duration) error {
	if value <= 0 {
		return fmt.Errorf("%s must be > 0", name)
	}
	return nil
}

func requireNonNegativeDurationValue(name string, value Duration, note string) error {
	if value < 0 {
		msg := fmt.Sprintf("%s must be >= 0", name)
		if strings.TrimSpace(note) != "" {
			msg += " (" + note + ")"
		}
		return errors.New(msg)
	}
	return nil
}

// resolve returns a kb.ShardingPolicy with user-supplied values overlaid on
// kb.DefaultShardingPolicy.
func (s ShardingConfig) resolve() kb.ShardingPolicy {
	p := kb.DefaultShardingPolicy()
	if s.ShardTriggerBytes != nil {
		p.ShardTriggerBytes = *s.ShardTriggerBytes
	}
	if s.ShardTriggerVectorRows != nil {
		p.ShardTriggerVectorRows = *s.ShardTriggerVectorRows
	}
	if s.TargetShardBytes != nil {
		p.TargetShardBytes = *s.TargetShardBytes
	}
	if s.MaxVectorRowsPerShard != nil {
		p.MaxVectorRowsPerShard = *s.MaxVectorRowsPerShard
	}
	if s.QueryShardFanout != nil {
		p.QueryShardFanout = *s.QueryShardFanout
	}
	if s.QueryShardFanoutAdaptiveMax != nil {
		p.QueryShardFanoutAdaptiveMax = *s.QueryShardFanoutAdaptiveMax
	}
	if s.QueryShardParallelism != nil {
		p.QueryShardParallelism = *s.QueryShardParallelism
	}
	if s.QueryShardLocalTopKMult != nil {
		p.QueryShardLocalTopKMult = *s.QueryShardLocalTopKMult
	}
	if s.SmallKBMaxShards != nil {
		p.SmallKBMaxShards = *s.SmallKBMaxShards
	}
	if s.CompactionEnabled != nil {
		p.CompactionEnabled = *s.CompactionEnabled
		p.CompactionEnabledSet = true
	}
	if s.CompactionMinShardCount != nil {
		p.CompactionMinShardCount = *s.CompactionMinShardCount
	}
	if s.CompactionTombstoneRatio != nil {
		p.CompactionTombstoneRatio = *s.CompactionTombstoneRatio
	}
	return p
}
