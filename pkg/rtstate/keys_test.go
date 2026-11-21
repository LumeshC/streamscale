package rtstate

import (
	"strings"
	"testing"
)

func TestKeysAreNamespaced(t *testing.T) {
	for _, k := range []string{
		SecondBucketKey(1), ActiveUsersKey(), UserStateKey("u"), AppliedKey("e"), UserRateKey("u", 1),
	} {
		if !strings.HasPrefix(k, "ss:") {
			t.Fatalf("key %q not namespaced", k)
		}
	}
	if SecondBucketKey(42) != "ss:rt:sec:42" {
		t.Fatalf("bucket key layout changed: %s (cross-service contract)", SecondBucketKey(42))
	}
}

func TestCentsString(t *testing.T) {
	cases := map[int64]string{123456: "1234.56", 5: "0.05", 0: "0.00", -250: "-2.50"}
	for in, want := range cases {
		if got := CentsString(in); got != want {
			t.Errorf("CentsString(%d)=%s want %s", in, got, want)
		}
	}
}
