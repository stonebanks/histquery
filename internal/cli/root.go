package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/stonebanks/histquery/internal/ingest/localgit"
	"github.com/stonebanks/histquery/internal/store/sqlite"
	"github.com/stonebanks/histquery/internal/store/syncstore"
)

type ctxKey struct{ name string }

var gitRepoKey = ctxKey{"gitRepo"}
var storeKey = ctxKey{"store"}

var rootCmd = &cobra.Command{
	Use:   "histquery",
	Short: "Turn your git commit history into a searchable memory",
	Long:  "Self-hosted CLI that turns your git commit history into a searchable memory. Local embeddings (Ollama), SQLite, no cloud, no accounts — ask natural-language questions and get cited answers from your own commits.",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		path, err := os.Getwd()
		if err != nil {
			return err
		}

		gitRepo, err := localgit.New(path)
		if err != nil {
			return err
		}

		appFilePath := filepath.Join(path, "."+cmd.Root().Name())

		dbPath := filepath.Join(appFilePath, "idx.db")
		sqliteRepo, err := sqlite.New(dbPath)
		if err != nil {
			return err
		}

		syncStore, err := syncstore.New(cmd.Context(), sqliteRepo, appFilePath)

		if err != nil {
			return fmt.Errorf("creating sync store: %w", err)
		}

		cmd.SetContext(context.WithValue(cmd.Context(), gitRepoKey, gitRepo))
		cmd.SetContext(context.WithValue(cmd.Context(), storeKey, syncStore))

		return err
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		gitRepo := cmd.Context().Value(gitRepoKey).(*localgit.Source)
		store := cmd.Context().Value(storeKey).(*syncstore.Store)

		err := store.Close()
		if err != nil {
			return err
		}

		err = gitRepo.Close()
		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
}

func Execute() error {
	return rootCmd.Execute()
}
