package database

import (
	"strings"
	"testing"
)

func TestToMySQLDSN_MariaDBURL(t *testing.T) {
	in := "mariadb://user:pass@localhost:3306/mydb"
	out, err := toMySQLDSN(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Basic shape
	if !strings.Contains(out, "user:pass@tcp(localhost:3306)/mydb") {
		t.Fatalf("dsn not converted properly: %s", out)
	}
	// Options we rely on
	if !strings.Contains(out, "parseTime=true") || !strings.Contains(out, "loc=UTC") {
		t.Fatalf("missing required options in dsn: %s", out)
	}
}

func TestToMySQLDSN_MySQLURL(t *testing.T) {
	in := "mysql://u:p@db.example:3307/ltv"
	out, err := toMySQLDSN(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "u:p@tcp(db.example:3307)/ltv") {
		t.Fatalf("dsn not converted properly: %s", out)
	}
	if !strings.Contains(out, "parseTime=true") || !strings.Contains(out, "loc=UTC") {
		t.Fatalf("missing required options in dsn: %s", out)
	}
}

func TestToMySQLDSN_Passthrough(t *testing.T) {
	// Already a native DSN (or anything else) should pass through unchanged
	in := "user:pass@tcp(127.0.0.1:3306)/db?parseTime=true&loc=UTC"
	out, err := toMySQLDSN(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != in {
		t.Fatalf("expected passthrough, got %q", out)
	}
}

func TestToMySQLDSN_Incomplete(t *testing.T) {
	_, err := toMySQLDSN("mariadb://user@/") // missing host/db
	if err == nil {
		t.Fatal("expected error for incomplete DSN, got nil")
	}
}
