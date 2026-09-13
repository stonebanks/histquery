package cli

import (
	"github.com/spf13/cobra"
	"github.com/stonebanks/histquery/internal/embed/ollama"
	"github.com/stonebanks/histquery/internal/search"
	"github.com/stonebanks/histquery/internal/store/syncstore"
)

func newSearchCmd() *cobra.Command {
	var opts search.Options

	cmd := &cobra.Command{
		Use:   "search [query]",
		Short: "Search index for given query",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSearch(cmd, args, &opts)
		},
	}

	cmd.Flags().IntVarP(&opts.TopK, "top", "k", 10, "number of top results to return")
	return cmd
}

func init() {
	rootCmd.AddCommand(newSearchCmd())
}

func runSearch(cmd *cobra.Command, args []string, opts *search.Options) error {
	query := search.Query(args[0])
	ctx := cmd.Context()

	store := cmd.Context().Value(storeKey).(*syncstore.Store)
	embedder := ollama.New("")

	s := search.New(embedder, store)
	err := s.Run(ctx, query, opts)
	if err != nil {
		return err
	}

	return nil
}
