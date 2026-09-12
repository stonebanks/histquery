package store

import "testing"

func TestNewCommitID(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid sha-1", "1234567890abcdef1234567890abcdef12345678", false},
		{"valid sha-256", "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", false},
		{"empty", "", true},
		{"too short", "abc123", true},
		{"uppercase", "1234567890ABCDEF1234567890abcdef12345678", true},
		{"non-hex chars", "123456789zabcdef1234567890abcdef123456zz", true},
		{"wrong length", "1234567890abcdef1234567890abcdef123456", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := NewCommitID(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NewCommitID(%q) = %v, want error", tt.input, id)
				}
				return
			}

			if err != nil {
				t.Fatalf("NewCommitID(%q) returned error: %v", tt.input, err)
			}
			if id.String() != tt.input {
				t.Errorf("String() = %q, want %q", id.String(), tt.input)
			}
		})
	}
}

func TestCommitID_ZeroValue(t *testing.T) {
	var id CommitID
	if id.String() != "" {
		t.Errorf("zero value String() = %q, want empty", id.String())
	}
}

func TestCommitID_Equality(t *testing.T) {
	a, err := NewCommitID("1234567890abcdef1234567890abcdef12345678")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewCommitID("1234567890abcdef1234567890abcdef12345678")
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewCommitID("abcdef1234567890abcdef1234567890abcdef12")
	if err != nil {
		t.Fatal(err)
	}

	if a != b {
		t.Errorf("expected equal CommitIDs built from the same SHA to compare equal")
	}
	if a == c {
		t.Errorf("expected different CommitIDs to compare unequal")
	}
}
