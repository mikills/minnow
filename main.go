package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mikills/minnow/cmd/configruntime"
	"github.com/mikills/minnow/kb/config"
)

func main() {
	logger := newLogger(os.Getenv("MINNOW_LOG_FORMAT"))

	args := os.Args[1:]
	if len(args) > 0 && args[0] == "config" {
		os.Exit(runConfigSubcommand(args[1:], logger))
	}

	if err := runServer(logger); err != nil {
		logger.Error("minnow exited with error", "error", err)
		os.Exit(1)
	}
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
		fmt.Fprintln(os.Stderr, "subcommands: validate [path]")
		return 2
	}

	switch args[0] {
	case "validate":
		return runConfigValidate(args[1:], logger)
	case "-h", "--help":
		fmt.Fprintln(os.Stderr, "usage: minnow config <subcommand>")
		fmt.Fprintln(os.Stderr, "subcommands: validate [path]")
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

func newLogger(format string) *slog.Logger {
	if format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, nil))
}
