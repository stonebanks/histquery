package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/stonebanks/histquery/internal/embed/ollama"
	"github.com/stonebanks/histquery/internal/indexer"
	"github.com/stonebanks/histquery/internal/ingest/localgit"
	"github.com/stonebanks/histquery/internal/store/sqlite"
)

var indexCmd = &cobra.Command{
	Use:   "index",
	Short: "Index git commit history into local searchable memory",
	RunE:  runIndex,
}

func init() {
	rootCmd.AddCommand(indexCmd)
}

func runIndex(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	source := cmd.Context().Value(gitRepoKey).(*localgit.Source)
	repo := cmd.Context().Value(sqliteRepoKey).(*sqlite.Repository)
	embedder := ollama.New("")

	idxr := indexer.New(source, embedder, repo)
	if err := idxr.Run(ctx, &indexer.IndexerOptions{}); err != nil {
		return fmt.Errorf("indexing: %w", err)
	}

	return nil
}
