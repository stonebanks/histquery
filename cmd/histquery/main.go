package main

import (
	"os"
	"path"

	"github.com/stonebanks/histquery/internal/search/sqlite"
)

func main() {

	userHomeDir, err := os.UserHomeDir()
	if err != nil {

		// TODO make something clever here
		panic(err)
	}
	dbPath := path.Join(userHomeDir, `/.histquery/sqlite.db`)
	_, err = sqlite.New(dbPath)
	if err != nil {
		panic(err)
	}
}
