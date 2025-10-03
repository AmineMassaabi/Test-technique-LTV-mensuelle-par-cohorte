package calculator

import (
	"testing"
	"time"
)

func TestParseMonth_Valid(t *testing.T) {
	got, err := parseMonth("032025")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestParseMonth_InvalidLength(t *testing.T) {
	_, err := parseMonth("32025") // 5 chars
	if err == nil {
		t.Fatal("expected error for invalid length, got nil")
	}
}

func TestParseMonth_InvalidMonth(t *testing.T) {
	_, err := parseMonth("132025") // 13th month
	if err == nil {
		t.Fatal("expected error for invalid month, got nil")
	}
}

func TestMonthsBetweenInclusive(t *testing.T) {
	start := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	got := monthsBetweenInclusive(start, end)
	if len(got) != 4 {
		t.Fatalf("got %d months, want 4", len(got))
	}
	// spot-check
	if got[0].Month() != time.March || got[3].Month() != time.June {
		t.Fatalf("unexpected months: %v", got)
	}
}

func TestFormatMonth(t *testing.T) {
	d := time.Date(2025, 11, 1, 0, 0, 0, 0, time.UTC)
	if fm := formatMonth(d); fm != "11/2025" {
		t.Fatalf("got %q, want %q", fm, "11/2025")
	}
}
