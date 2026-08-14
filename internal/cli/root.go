package cli

import (
	"os"
	"path"

	"github.com/spf13/cobra"
	"github.com/stonebanks/histquery/internal/store"
	"github.com/stonebanks/histquery/internal/store/sqlite"
)

var repo *store.Repository

var rootCmd = &cobra.Command{
	Use:   "histquery",
	Short: "Turn your git commit history into a searchable memory",
	Long:  "Self-hosted CLI that turns your git commit history into a searchable memory. Local embeddings (Ollama), SQLite, no cloud, no accounts — ask natural-language questions and get cited answers from your own commits.",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		path, err := dbPath()
		if err != nil {
			return err
		}
		repo, err = sqlite.New(path)
		return err
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		return repo.Db.Close()
	},
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
}

func Execute() error {
	return rootCmd.Execute()
}

func dbPath() (string, error) {
	userHomeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return path.Join(userHomeDir, ".histquery", "sqlite.db"), nil
}
