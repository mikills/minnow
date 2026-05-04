package mcpserver

import (
	"time"

	"github.com/mikills/minnow/kb"
)

type CodeIndexDefaults struct {
	Include          []string
	Exclude          []string
	MaxFileBytes     int64
	ChunkSize        int
	ChunkOverlap     int
	IncludeUntracked bool
	ResourcePolicy   kb.CodeIndexResourcePolicy
	RequireConfirm   bool
}

type Config struct {
	Enabled            bool
	HTTPEnabled        bool
	StdioEnabled       bool
	HTTPPath           string
	ReadOnly           bool
	AllowIndexing      bool
	AllowSyncIndexing  bool
	AllowDestructive   bool
	AllowAdmin         bool
	DefaultSyncTimeout time.Duration
	MaxSyncTimeout     time.Duration
	HTTPJSONResponse   bool
	HTTPStateless      bool
	CodeIndex          CodeIndexDefaults
}

func (c Config) normalized() Config {
	if c.HTTPPath == "" {
		c.HTTPPath = "/mcp"
	}
	if c.DefaultSyncTimeout <= 0 {
		c.DefaultSyncTimeout = 30 * time.Second
	}
	if c.MaxSyncTimeout <= 0 {
		c.MaxSyncTimeout = 2 * time.Minute
	}
	if c.DefaultSyncTimeout > c.MaxSyncTimeout {
		c.DefaultSyncTimeout = c.MaxSyncTimeout
	}
	return c
}
