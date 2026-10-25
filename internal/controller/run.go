package controller

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/SimonSchneider/goslu/srvu"
	"github.com/SimonSchneider/goslu/templ"
	"github.com/SimonSchneider/pefigo"
	"github.com/SimonSchneider/pefigo/internal/model"
)

func Run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer, getEnv func(string) string, getwd func() (string, error)) error {
	cfg, err := model.ParseConfig(args[1:], getEnv)
	if err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, os.Kill)
	defer cancel()
	logger := srvu.LogToOutput(log.New(stdout, "", log.LstdFlags|log.Lshortfile))

	svc, err := model.Open(ctx, pefigo.StaticEmbeddedFS, "static/migrations", cfg.DbURL)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}

	runner := model.NewForecastRunner(5*time.Second, func(ctx context.Context) {
		if err := svc.RunForecastCache(ctx); err != nil {
			logger.Printf("forecast cache error: %v", err)
		}
	})
	svc.SetForecastRunner(runner)
	defer runner.Stop()

	// Trigger initial forecast on startup
	runner.Invalidate()

	public, _, err := templ.GetPublicAndTemplates(pefigo.StaticEmbeddedFS, &templ.Config{
		Watch: cfg.Watch,
	})
	if err != nil {
		return fmt.Errorf("sub static: %w", err)
	}

	srv := &http.Server{
		BaseContext: func(listener net.Listener) context.Context {
			return ctx
		},
		Addr:    cfg.Addr,
		Handler: srvu.With(NewHandler(svc, public), http.NewCrossOriginProtection().Handler, srvu.WithCompression(), srvu.WithLogger(logger)),
	}
	logger.Printf("starting chore server, listening on %s\n  sqliteDB: %s", cfg.Addr, cfg.DbURL)
	return srvu.RunServerGracefully(ctx, srv, logger)
}
