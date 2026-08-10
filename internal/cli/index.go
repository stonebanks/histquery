package cli

import (
	"github.com/spf13/cobra"
)

var indexCmd = &cobra.Command{
	Use:   "index",
	Short: "Index git commit history into local searchable memory",
	RunE: func(cmd *cobra.Command, args []string) error {
		// TODO: walk git log, embed commits, and store them via repo.
		return nil
	},
}

func init() {
	rootCmd.AddCommand(indexCmd)
}
