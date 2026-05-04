package kb

import (
	"context"
	"fmt"
	"runtime"
	"syscall"
	"time"
)

const (
	DefaultCodeEmbedBatchSize = 32
	DefaultCodeMaxBatchBytes  = 256 * 1024
	DefaultCodeMaxHeapBytes   = 1024 * 1024 * 1024
	DefaultCodeMaxRSSBytes    = 1024 * 1024 * 1024
	DefaultCodeThrottle       = 100 * time.Millisecond
	DefaultCodeLargeRepoFiles = 1000
)

// CodeIndexResourcePolicy is the Interface for developer-machine safety during
// code indexing. It centralizes batch sizing, throttling, and memory guards so
// CLI, MCP, hooks, and tests do not each invent their own resource rules.
type CodeIndexResourcePolicy struct {
	EmbedBatchSize int
	MaxBatchBytes  int
	Throttle       time.Duration
	MaxHeapBytes   uint64
	MaxRSSBytes    uint64
	LargeRepoFiles int
}

func codeIndexResourcePolicyFromOptions(opts CodeIndexOptions) CodeIndexResourcePolicy {
	policy := CodeIndexResourcePolicy{
		EmbedBatchSize: opts.EmbedBatchSize,
		MaxBatchBytes:  opts.MaxBatchBytes,
		Throttle:       opts.Throttle,
		MaxHeapBytes:   opts.MaxHeapBytes,
		MaxRSSBytes:    opts.MaxRSSBytes,
		LargeRepoFiles: opts.LargeRepoFiles,
	}
	return policy.Normalize()
}

func (p CodeIndexResourcePolicy) Normalize() CodeIndexResourcePolicy {
	if p.EmbedBatchSize <= 0 {
		p.EmbedBatchSize = DefaultCodeEmbedBatchSize
	}
	if p.MaxBatchBytes <= 0 {
		p.MaxBatchBytes = DefaultCodeMaxBatchBytes
	}
	if p.Throttle < 0 {
		p.Throttle = 0
	} else if p.Throttle == 0 {
		p.Throttle = DefaultCodeThrottle
	}
	if p.MaxHeapBytes == 0 {
		p.MaxHeapBytes = DefaultCodeMaxHeapBytes
	}
	if p.MaxRSSBytes == 0 {
		p.MaxRSSBytes = DefaultCodeMaxRSSBytes
	}
	if p.LargeRepoFiles <= 0 {
		p.LargeRepoFiles = DefaultCodeLargeRepoFiles
	}
	return p
}

func (p CodeIndexResourcePolicy) ApplyToOptions(opts CodeIndexOptions) CodeIndexOptions {
	p = p.Normalize()
	opts.EmbedBatchSize = p.EmbedBatchSize
	opts.MaxBatchBytes = p.MaxBatchBytes
	opts.Throttle = p.Throttle
	opts.MaxHeapBytes = p.MaxHeapBytes
	opts.MaxRSSBytes = p.MaxRSSBytes
	opts.LargeRepoFiles = p.LargeRepoFiles
	return opts
}

func (p CodeIndexResourcePolicy) Validate(prefix string) error {
	if prefix == "" {
		prefix = "code_index"
	}
	if p.EmbedBatchSize <= 0 {
		return fmt.Errorf("%s.embed_batch_size must be > 0, got %d", prefix, p.EmbedBatchSize)
	}
	if p.MaxBatchBytes <= 0 {
		return fmt.Errorf("%s.max_batch_bytes must be > 0, got %d", prefix, p.MaxBatchBytes)
	}
	if p.Throttle < 0 {
		return fmt.Errorf("%s.throttle must be >= 0", prefix)
	}
	if p.MaxHeapBytes == 0 {
		return fmt.Errorf("%s.max_heap_bytes must be > 0, got %d", prefix, p.MaxHeapBytes)
	}
	if p.MaxRSSBytes == 0 {
		return fmt.Errorf("%s.max_rss_bytes must be > 0, got %d", prefix, p.MaxRSSBytes)
	}
	if p.LargeRepoFiles <= 0 {
		return fmt.Errorf("%s.large_repo_files must be > 0, got %d", prefix, p.LargeRepoFiles)
	}
	return nil
}

func (p CodeIndexResourcePolicy) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.MaxHeapBytes > 0 {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		if mem.Sys > p.MaxHeapBytes {
			return fmt.Errorf("code index resource guard: heap/system memory %d bytes exceeds max_heap_bytes %d", mem.Sys, p.MaxHeapBytes)
		}
	}
	if p.MaxRSSBytes > 0 {
		rss, ok := currentProcessRSSBytes()
		if ok && rss > p.MaxRSSBytes {
			return fmt.Errorf("code index resource guard: resident memory %d bytes exceeds max_rss_bytes %d", rss, p.MaxRSSBytes)
		}
	}
	return nil
}

func (p CodeIndexResourcePolicy) ThrottleBatch(ctx context.Context) error {
	if p.Throttle <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(p.Throttle)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p CodeIndexResourcePolicy) BatchEnd(docs []Document) int {
	if len(docs) == 0 {
		return 0
	}
	end := p.EmbedBatchSize
	if end <= 0 || end > len(docs) {
		end = len(docs)
	}
	if p.MaxBatchBytes <= 0 {
		return end
	}
	bytes := 0
	for i, doc := range docs[:end] {
		if i > 0 && bytes+len(doc.Text) > p.MaxBatchBytes {
			return i
		}
		bytes += len(doc.Text)
	}
	return end
}

func documentsTextBytes(docs []Document) int {
	total := 0
	for _, doc := range docs {
		total += len(doc.Text)
	}
	return total
}

func currentProcessRSSBytes() (uint64, bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, false
	}
	rss := uint64(usage.Maxrss)
	if runtime.GOOS == "linux" {
		rss *= 1024
	}
	return rss, rss > 0
}
