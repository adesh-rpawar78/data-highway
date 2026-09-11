// Command server runs the Data Highway ingestion service.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"datahighway/internal/httpapi"
	"datahighway/internal/pipeline"
	"datahighway/internal/store"
)

func main() {
	logger := log.New(os.Stdout, "datahighway ", log.LstdFlags|log.Lmicroseconds)

	port := getEnv("PORT", "8080")
	workers := getEnvInt("WORKERS", 64)
	queueSize := getEnvInt("QUEUE_SIZE", 10000)
	apiKey := os.Getenv("API_KEY")

	st, err := store.NewRedisStore(getEnv("REDIS_ADDR", "localhost:6379"), "", 0)
	if err != nil {
		logger.Fatalf("connect to redis: %v", err)
	}
	defer st.Close()

	p := pipeline.New(workers, queueSize, st, logger)
	p.Start(context.Background())

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      httpapi.NewServer(p, apiKey, logger),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Printf("listening on :%s (workers=%d queue=%d api_key_set=%v)", port, workers, queueSize, apiKey != "")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	logger.Println("shutdown signal received, draining...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Printf("http shutdown error: %v", err)
	}
	if err := p.Shutdown(shutdownCtx); err != nil {
		logger.Printf("pipeline shutdown error: %v", err)
	}

	stats := p.Stats()
	logger.Printf("done. received=%d processed=%d failed=%d", stats.Received, stats.Processed, stats.Failed)
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
