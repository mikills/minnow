package main

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	appcmd "github.com/mikills/minnow/cmd"
	"github.com/mikills/minnow/cmd/configruntime"
	"github.com/mikills/minnow/kb/config"
	"github.com/mikills/minnow/mcpserver"
)

func main() {
	logger := newLogger(os.Getenv("MINNOW_LOG_FORMAT"))

	args := os.Args[1:]
	if len(args) > 0 && args[0] == "mcp" {
		os.Exit(runMCPSubcommand(args[1:]))
	}
	if len(args) > 0 && args[0] == "config" {
		os.Exit(runConfigSubcommand(args[1:], logger))
	}

	if err := runServer(logger); err != nil {
		logger.Error("minnow exited with error", "error", err)
		os.Exit(1)
	}
}

func runMCPSubcommand(args []string) int {
	if len(args) == 0 || args[0] != "stdio" {
		fmt.Fprintln(os.Stderr, "usage: minnow mcp stdio")
		return 2
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := config.Load(os.Getenv("MINNOW_CONFIG"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	rt, err := configruntime.Build(ctx, cfg, configruntime.BuildOptions{Logger: logger})
	if err != nil {
		fmt.Fprintf(os.Stderr, "build runtime: %v\n", err)
		return 1
	}
	mcpCfg := rt.MCPConfig()
	if !mcpCfg.Enabled || !mcpCfg.StdioEnabled {
		fmt.Fprintln(os.Stderr, "mcp stdio transport is not enabled")
		return 1
	}
	if err := rt.StartBackground(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "start runtime: %v\n", err)
		return 1
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTPShutdownTimeout())
		defer cancel()
		_ = rt.Stop(shutdownCtx)
	}()
	server := appcmd.NewMCPServerFromKB(rt.KB(), mcpCfg, logger)
	if err := mcpserver.RunStdio(ctx, server); err != nil {
		fmt.Fprintf(os.Stderr, "mcp stdio failed: %v\n", err)
		return 1
	}
	return 0
}

// runServer loads the YAML config, builds the runtime, and serves HTTP until
// SIGINT/SIGTERM. This is the only entry point that binds ports and connects
// to external services.
func runServer(logger *slog.Logger) error {
	cfg, err := config.Load(os.Getenv("MINNOW_CONFIG"))
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rt, err := configruntime.Build(ctx, cfg, configruntime.BuildOptions{Logger: logger})
	if err != nil {
		return fmt.Errorf("build runtime: %w", err)
	}

	if err := rt.Start(ctx); err != nil {
		return fmt.Errorf("start runtime: %w", err)
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTPShutdownTimeout())
		defer cancel()
		_ = rt.Stop(shutdownCtx)
	}()

	return rt.Wait()
}

// runConfigSubcommand implements the `minnow config ...` CLI. Today the only
// leaf is `validate`, which runs Load + Build(DryRun=true) and exits 0/1.
func runConfigSubcommand(args []string, logger *slog.Logger) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: minnow config <subcommand>")
		fmt.Fprintln(os.Stderr, "subcommands: validate [path], init dev-openai [path] [--force]")
		return 2
	}

	switch args[0] {
	case "validate":
		return runConfigValidate(args[1:], logger)
	case "init":
		return runConfigInit(args[1:])
	case "-h", "--help":
		fmt.Fprintln(os.Stderr, "usage: minnow config <subcommand>")
		fmt.Fprintln(os.Stderr, "subcommands: validate [path], init dev-openai [path] [--force]")
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown config subcommand: %s\n", args[0])
		return 2
	}
}

func runConfigValidate(args []string, logger *slog.Logger) int {
	path := os.Getenv("MINNOW_CONFIG")
	if len(args) >= 1 {
		path = args[0]
	}

	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config invalid: %v\n", err)
		return 1
	}

	if _, err := configruntime.Build(context.Background(), cfg, configruntime.BuildOptions{DryRun: true, Logger: logger}); err != nil {
		fmt.Fprintf(os.Stderr, "config build failed: %v\n", err)
		return 1
	}

	fmt.Println("config OK")
	return 0
}

func runConfigInit(args []string) int {
	if len(args) == 0 || args[0] != "dev-openai" {
		fmt.Fprintln(os.Stderr, "usage: minnow config init dev-openai [path] [--force]")
		return 2
	}
	path := ""
	force := false
	for _, arg := range args[1:] {
		switch {
		case arg == "--force":
			force = true
		case strings.HasPrefix(arg, "-"):
			fmt.Fprintf(os.Stderr, "unknown flag: %s\n", arg)
			return 2
		case path == "":
			path = arg
		default:
			fmt.Fprintln(os.Stderr, "usage: minnow config init dev-openai [path] [--force]")
			return 2
		}
	}
	if path == "" {
		var err error
		path, err = config.UserConfigPath()
		if err != nil {
			fmt.Fprintf(os.Stderr, "resolve user config path: %v\n", err)
			return 1
		}
	}
	if err := writeConfigTemplate(path, devOpenAIConfigTemplate(), force); err != nil {
		fmt.Fprintf(os.Stderr, "write config: %v\n", err)
		return 1
	}
	fmt.Printf("wrote %s\n", path)
	return 0
}

func writeConfigTemplate(path string, data []byte, force bool) error {
	if !force {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%s already exists (use --force to overwrite)", path)
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	flag := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if !force {
		flag = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	}
	f, err := os.OpenFile(path, flag, filePermOwnerReadWrite)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	return err
}

const filePermOwnerReadWrite fs.FileMode = 0o600

func devOpenAIConfigTemplate() []byte {
	return []byte(`# Minnow OpenAI-backed developer config.
# Set OPENAI_API_KEY in the environment used by your terminal or MCP client.

storage:
  blob:
    root: ./blobs
  cache:
    dir: ./cache

format:
  duckdb:
    extension_dir: ./extensions
    offline: false

embedder:
  provider: openai_compatible
  openai_compatible:
    base_url: https://api.openai.com/v1
    model: text-embedding-3-small
    token: ${OPENAI_API_KEY}
    dimensions: 0

mcp:
  enabled: true
  transports: [http, stdio]
  http_path: /mcp
  allow_indexing: true
  allow_sync_indexing: true
`)
}

func newLogger(format string) *slog.Logger {
	if format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, nil))
}
