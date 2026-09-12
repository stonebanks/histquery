package store

import (
	"fmt"
	"regexp"
)

var commitIDPattern = regexp.MustCompile(`^[0-9a-f]{40}$|^[0-9a-f]{64}$`)

// CommitID is a validated git commit SHA (SHA-1 or SHA-256, lowercase hex).
// The zero value is invalid; construct one with NewCommitID.
type CommitID struct {
	value string
}

func NewCommitID(s string) (CommitID, error) {
	if !commitIDPattern.MatchString(s) {
		return CommitID{}, fmt.Errorf("invalid commit SHA %q", s)
	}
	return CommitID{value: s}, nil
}

func (c CommitID) String() string {
	return c.value
}
