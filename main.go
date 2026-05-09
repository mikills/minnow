package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	appcmd "github.com/mikills/minnow/cmd"
	"github.com/mikills/minnow/cmd/configruntime"
	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/config"
	"github.com/mikills/minnow/mcpserver"
)

var backgroundContext = context.Background()

const version = "v0.1.0"

func main() {
	logger := newLogger(os.Getenv("MINNOW_LOG_FORMAT"))
	ctx := backgroundContext
	if code, handled := runTopLevelCommand(ctx, os.Args[1:], logger); handled {
		os.Exit(code)
	}
	if err := runServer(ctx, logger); err != nil {
		logger.Error("minnow exited with error", "error", err)
		os.Exit(1)
	}
}

func runTopLevelCommand(ctx context.Context, args []string, logger *slog.Logger) (int, bool) {
	if len(args) == 0 {
		return 0, false
	}
	switch args[0] {
	case "--version", "version":
		fmt.Println("minnow " + version)
		return 0, true
	case "-h", "--help":
		printUsage()
		return 0, true
	case "mcp":
		return runMCPSubcommand(ctx, args[1:]), true
	case "index":
		return runIndexSubcommand(ctx, args[1:], logger), true
	case "config":
		return runConfigSubcommand(ctx, args[1:], logger), true
	case "setup":
		return runSetupSubcommand(args[1:]), true
	default:
		return 0, false
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: minnow [mcp|index|config|setup|version]")
	fmt.Fprintln(os.Stderr, "       minnow --version")
	fmt.Fprintln(os.Stderr, "       minnow mcp stdio")
	fmt.Fprintln(os.Stderr, "       minnow index <codebase|refresh|status|hooks>")
	fmt.Fprintln(os.Stderr, "       minnow config <validate|init>")
	fmt.Fprintln(os.Stderr, "       minnow setup")
}

func runMCPSubcommand(baseCtx context.Context, args []string) int {
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
	ctx, stop := signal.NotifyContext(baseCtx, os.Interrupt, syscall.SIGTERM)
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
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.HTTPShutdownTimeout())
		defer cancel()
		if err := rt.Stop(shutdownCtx); err != nil {
			logger.Warn("runtime stop failed", "error", err)
		}
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
func runServer(baseCtx context.Context, logger *slog.Logger) error {
	cfg, err := config.Load(os.Getenv("MINNOW_CONFIG"))
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(baseCtx, os.Interrupt, syscall.SIGTERM)
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
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.HTTPShutdownTimeout())
		defer cancel()
		if err := rt.Stop(shutdownCtx); err != nil {
			logger.Warn("runtime stop failed", "error", err)
		}
	}()

	return rt.Wait()
}

// runConfigSubcommand implements the `minnow config ...` CLI. Today the only
// leaf is `validate`, which runs Load + Build(DryRun=true) and exits 0/1.
func runConfigSubcommand(ctx context.Context, args []string, logger *slog.Logger) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: minnow config <subcommand>")
		fmt.Fprintln(os.Stderr, "subcommands: validate [path], init dev-openai [path] [--force]")
		return 2
	}

	switch args[0] {
	case "validate":
		return runConfigValidate(ctx, args[1:], logger)
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

func runConfigValidate(ctx context.Context, args []string, logger *slog.Logger) int {
	path := os.Getenv("MINNOW_CONFIG")
	if len(args) >= 1 {
		path = args[0]
	}

	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config invalid: %v\n", err)
		return 1
	}

	if _, err := configruntime.Build(ctx, cfg, configruntime.BuildOptions{DryRun: true, Logger: logger}); err != nil {
		fmt.Fprintf(os.Stderr, "config build failed: %v\n", err)
		return 1
	}

	fmt.Println("config OK")
	return 0
}

func runConfigInit(args []string) int {
	path, force, code, ok := parseConfigInitArgs(args)
	if !ok {
		return code
	}
	if path == "" {
		resolved, err := config.UserConfigPath()
		if err != nil {
			fmt.Fprintf(os.Stderr, "resolve user config path: %v\n", err)
			return 1
		}
		path = resolved
	}
	if err := writeConfigTemplate(path, devOpenAIConfigTemplate(), force); err != nil {
		fmt.Fprintf(os.Stderr, "write config: %v\n", err)
		return 1
	}
	fmt.Printf("wrote %s\n", path)
	return 0
}

func parseConfigInitArgs(args []string) (string, bool, int, bool) {
	if len(args) == 0 || args[0] != "dev-openai" {
		fmt.Fprintln(os.Stderr, "usage: minnow config init dev-openai [path] [--force]")
		return "", false, 2, false
	}
	path := ""
	force := false
	for _, arg := range args[1:] {
		parsedPath, parsedForce, code, ok := parseConfigInitArg(arg, path, force)
		if !ok {
			return "", false, code, false
		}
		path, force = parsedPath, parsedForce
	}
	return path, force, 0, true
}

func parseConfigInitArg(arg string, path string, force bool) (string, bool, int, bool) {
	switch {
	case arg == "--force":
		return path, true, 0, true
	case strings.HasPrefix(arg, "-"):
		fmt.Fprintf(os.Stderr, "unknown flag: %s\n", arg)
		return "", false, 2, false
	case path == "":
		return arg, force, 0, true
	default:
		fmt.Fprintln(os.Stderr, "usage: minnow config init dev-openai [path] [--force]")
		return "", false, 2, false
	}
}

func runIndexSubcommand(ctx context.Context, args []string, logger *slog.Logger) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: minnow index <codebase|refresh|status|hooks>")
		return 2
	}
	switch args[0] {
	case "codebase", "refresh":
		return runIndexRefresh(ctx, args[1:], logger)
	case "status":
		return runIndexStatus(ctx, args[1:], logger)
	case "hooks":
		return runIndexHooks(ctx, args[1:])
	case "-h", "--help":
		fmt.Fprintln(os.Stderr, "usage: minnow index <codebase|refresh|status|hooks>")
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown index subcommand: %s\n", args[0])
		return 2
	}
}

type indexCLIOptions struct {
	kbID             string
	indexKey         string
	description      string
	root             string
	binary           string
	includeUntracked bool
	quiet            bool
	force            bool
	yes              bool
	lowResource      bool
	embedBatchSize   int
	maxBatchBytes    int
	maxHeapBytes     uint64
	maxRSSBytes      uint64
	largeRepoFiles   int
	throttle         time.Duration
}

func parseIndexCLIOptions(args []string) (indexCLIOptions, error) {
	opts := indexCLIOptions{indexKey: "default", root: os.Getenv("MINNOW_REPO_ROOT")}
	if opts.root == "" {
		opts.root = "."
	}

	fs := flag.NewFlagSet("minnow index", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.kbID, "kb", opts.kbID, "knowledge base id")
	fs.StringVar(&opts.indexKey, "index-key", opts.indexKey, "code index registry key")
	fs.StringVar(&opts.description, "description", opts.description, "code index description")
	fs.StringVar(&opts.root, "root", opts.root, "repository root")
	fs.BoolVar(&opts.includeUntracked, "include-untracked", opts.includeUntracked, "include untracked git files")
	fs.StringVar(&opts.binary, "binary", opts.binary, "minnow binary path for hooks")
	fs.BoolVar(&opts.quiet, "quiet", opts.quiet, "suppress JSON output")
	fs.BoolVar(&opts.force, "force", opts.force, "force operation or confirm large indexes")
	fs.BoolVar(&opts.yes, "yes", opts.yes, "confirm prompts")
	fs.BoolVar(&opts.yes, "y", opts.yes, "confirm prompts")
	fs.BoolVar(&opts.lowResource, "low-resource", opts.lowResource, "use conservative indexing resource defaults")
	fs.IntVar(&opts.embedBatchSize, "batch-size", opts.embedBatchSize, "embedding batch size")
	fs.IntVar(&opts.maxBatchBytes, "max-batch-bytes", opts.maxBatchBytes, "maximum text bytes per embedding batch")
	fs.Uint64Var(&opts.maxHeapBytes, "max-heap-bytes", opts.maxHeapBytes, "maximum Go heap/system bytes")
	fs.Uint64Var(&opts.maxRSSBytes, "max-rss-bytes", opts.maxRSSBytes, "maximum resident set bytes")
	fs.IntVar(&opts.largeRepoFiles, "large-repo-files", opts.largeRepoFiles, "large repository confirmation threshold")
	fs.DurationVar(&opts.throttle, "throttle", opts.throttle, "delay between embedding batches")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if fs.NArg() > 0 {
		return opts, fmt.Errorf("unexpected argument: %s", fs.Arg(0))
	}
	return opts, validateIndexCLIOptions(opts)
}

func validateIndexCLIOptions(opts indexCLIOptions) error {
	return firstCLIValidationErr(
		validateOptionalCLIValue("--kb", opts.kbID),
		validateRequiredCLIValue("--index-key", opts.indexKey),
		validateRequiredCLIValue("--root", opts.root),
		validateOptionalCLIValue("--binary", opts.binary),
		validateNonNegativeIndexNumbers(opts),
		validateNonNegativeDuration("--throttle", opts.throttle),
	)
}

func firstCLIValidationErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func validateOptionalCLIValue(name string, value string) error {
	if value != "" && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s requires a value", name)
	}
	return nil
}

func validateRequiredCLIValue(name string, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s requires a value", name)
	}
	return nil
}

func validateNonNegativeIndexNumbers(opts indexCLIOptions) error {
	if opts.embedBatchSize < 0 || opts.maxBatchBytes < 0 || opts.largeRepoFiles < 0 {
		return fmt.Errorf("numeric index flags must be non-negative")
	}
	return nil
}

func validateNonNegativeDuration(name string, value time.Duration) error {
	if value < 0 {
		return fmt.Errorf("%s must be a non-negative duration", name)
	}
	return nil
}

func buildRuntimeForCLI(ctx context.Context, logger *slog.Logger) (*config.Config, *configruntime.Runtime, error) {
	cfg, err := config.Load(os.Getenv("MINNOW_CONFIG"))
	if err != nil {
		return nil, nil, fmt.Errorf("load config: %w", err)
	}
	rt, err := configruntime.Build(ctx, cfg, configruntime.BuildOptions{Logger: logger})
	if err != nil {
		return nil, nil, fmt.Errorf("build runtime: %w", err)
	}
	if err := rt.StartBackground(ctx); err != nil {
		return nil, nil, fmt.Errorf("start runtime: %w", err)
	}
	return cfg, rt, nil
}

func runIndexRefresh(ctx context.Context, args []string, logger *slog.Logger) int {
	opts, err := parseIndexCLIOptions(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 2
	}
	cfg, rt, err := buildRuntimeForCLI(ctx, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	defer stopRuntimeForCLI(ctx, logger, rt)

	result, err := rt.KB().IndexCodebase(ctx, codeIndexOptionsForCLI(cfg, opts))
	if err != nil {
		fmt.Fprintf(os.Stderr, "index codebase: %v\n", err)
		return 1
	}
	if !opts.quiet {
		if err := writeJSON(result); err != nil {
			fmt.Fprintf(os.Stderr, "write json: %v\n", err)
			return 1
		}
	}
	return 0
}

func stopRuntimeForCLI(ctx context.Context, logger *slog.Logger, rt *configruntime.Runtime) {
	if err := rt.Stop(context.WithoutCancel(ctx)); err != nil {
		logger.Warn("runtime stop failed", "error", err)
	}
}

func codeIndexOptionsForCLI(cfg *config.Config, opts indexCLIOptions) kb.CodeIndexOptions {
	indexOpts := configruntime.CodeIndexOptionsFromConfig(cfg, opts.kbID, opts.root)
	indexOpts.IndexKey = opts.indexKey
	indexOpts.Description = opts.description
	indexOpts.ConfirmedLarge = opts.yes || opts.force
	if opts.includeUntracked {
		indexOpts.IncludeUntracked = true
	}
	applyLowResourceCLIOptions(&opts)
	applyCLIResourceOverrides(&indexOpts, opts)
	return indexOpts
}

func applyLowResourceCLIOptions(opts *indexCLIOptions) {
	if !opts.lowResource {
		return
	}
	if opts.embedBatchSize == 0 {
		opts.embedBatchSize = 16
	}
	if opts.maxBatchBytes == 0 {
		opts.maxBatchBytes = 128 * 1024
	}
	if opts.throttle == 0 {
		opts.throttle = 250 * time.Millisecond
	}
}

func applyCLIResourceOverrides(indexOpts *kb.CodeIndexOptions, opts indexCLIOptions) {
	if opts.embedBatchSize > 0 {
		indexOpts.EmbedBatchSize = opts.embedBatchSize
	}
	if opts.maxBatchBytes > 0 {
		indexOpts.MaxBatchBytes = opts.maxBatchBytes
	}
	if opts.maxHeapBytes > 0 {
		indexOpts.MaxHeapBytes = opts.maxHeapBytes
	}
	if opts.maxRSSBytes > 0 {
		indexOpts.MaxRSSBytes = opts.maxRSSBytes
	}
	if opts.largeRepoFiles > 0 {
		indexOpts.LargeRepoFiles = opts.largeRepoFiles
	}
	if opts.throttle > 0 {
		indexOpts.Throttle = opts.throttle
	}
}

func runIndexStatus(ctx context.Context, args []string, logger *slog.Logger) int {
	opts, err := parseIndexCLIOptions(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 2
	}
	_, rt, err := buildRuntimeForCLI(ctx, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	defer stopRuntimeForCLI(ctx, logger, rt)
	selection, err := kb.ResolveCodeIndexSelection(opts.root, opts.indexKey, opts.kbID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve code index: %v\n", err)
		return 1
	}
	status, err := rt.KB().CodeIndexStatus(ctx, selection.KBID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "code index status: %v\n", err)
		return 1
	}
	status.IndexKey = selection.IndexKey
	status.Description = selection.Description
	writeJSON(status)
	return 0
}

func runIndexHooks(ctx context.Context, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(
			os.Stderr,
			"usage: minnow index hooks <install|uninstall|status> [--kb id] [--index-key key] [--root path] [--binary minnow] [--force]",
		)
		return 2
	}
	action := args[0]
	opts, err := parseIndexCLIOptions(args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 2
	}
	var status any
	switch action {
	case "install":
		selection, selErr := kb.ResolveCodeIndexSelection(opts.root, opts.indexKey, opts.kbID)
		if selErr != nil {
			fmt.Fprintf(os.Stderr, "resolve code index: %v\n", selErr)
			return 1
		}
		status, err = kb.InstallCodeIndexHooks(
			ctx,
			kb.CodeHookOptions{
				Root:     opts.root,
				KBID:     selection.KBID,
				IndexKey: selection.IndexKey,
				Binary:   opts.binary,
				Force:    opts.force,
			},
		)
	case "uninstall":
		status, err = kb.UninstallCodeIndexHooks(ctx, opts.root)
	case "status":
		status, err = kb.CodeIndexHookStatus(ctx, opts.root)
	default:
		fmt.Fprintf(os.Stderr, "unknown hooks subcommand: %s\n", action)
		return 2
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "index hooks %s: %v\n", action, err)
		return 1
	}
	if err := writeJSON(status); err != nil {
		fmt.Fprintf(os.Stderr, "write json: %v\n", err)
		return 1
	}
	return 0
}

func writeJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
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

code_index:
  include: ["**/*"]
  max_file_bytes: 1048576
  chunk_size: 1200
  chunk_overlap: 120
  include_untracked: false

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
