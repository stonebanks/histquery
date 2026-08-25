package localgit

import (
	"context"
	"fmt"
	"io"

	"github.com/go-git/go-git/v6"
	"github.com/stonebanks/histquery/internal/ingest"
)

type Source struct {
	repo *git.Repository
}

func New(gitRepositoryPath string) (*Source, error) {
	r, err := git.PlainOpen(gitRepositoryPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open repository: %w", err)
	}

	return &Source{
		repo: r,
	}, nil
}

func (l *Source) Close() error {
	err := l.repo.Close()
	return err
}

func (l *Source) StreamCommits(ctx context.Context, out chan<- ingest.Commit) error {

	iter, err := l.repo.Log(&git.LogOptions{
		Order: git.LogOrderDefault,
		All:   true,
	})
	if err != nil {
		return fmt.Errorf("failed to open repository: %w", err)
	}
	defer iter.Close()

	for {
		commit, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to open repository: %w", err)
		}

		// filter out merge commits
		if commit.NumParents() > 1 {
			continue
		}



		out <- ingest.Commit{
			SHA:  commit.Hash.String(),
			Body: commit.Message,
			Author: ingest.Developer{
				Name:  commit.Author.Name,
				Email: commit.Author.Email,
			},
			AuthorDate: commit.Author.When,
			Committer: ingest.Developer{
				Name:  commit.Committer.Name,
				Email: commit.Committer.Email,
			},
			CommitterDate: commit.Committer.When,
		}
	}

	return nil
}
