package config

import (
	"strings"
	"testing"
)

func TestSplitCSVViaFlagDefault(t *testing.T) {
	parts := splitCSV("a:9092, b:9092")
	if len(parts) != 2 || parts[0] != "a:9092" || parts[1] != "b:9092" {
		t.Fatalf("got %#v", parts)
	}
}

func TestJoin(t *testing.T) {
	if join([]string{"x", "y"}) != "x,y" {
		t.Fatal(join([]string{"x", "y"}))
	}
}

func TestEnvCSVTrim(t *testing.T) {
	if strings.Contains(join([]string{"localhost:9092"}), " ") {
		t.Fatal("unexpected space")
	}
}
