// cmd/showcase-runner/membership_test.go
package main

import "testing"

func TestParseMembersResponse_AllUp(t *testing.T) {
	body := `{"members":[
		{"node":"pekko://X@127.0.0.1:2541","status":"Up"},
		{"node":"pekko://X@127.0.0.1:2551","status":"Up"}],
	  "unreachable":[]}`
	st, err := parseMembersJSON([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if st.upCount != 2 {
		t.Fatalf("upCount=%d want 2", st.upCount)
	}
	if len(st.unreachable) != 0 {
		t.Fatalf("want 0 unreachable, got %d", len(st.unreachable))
	}
}

func TestParseMembersResponse_MixedStatus(t *testing.T) {
	body := `{"members":[
		{"node":"pekko://X@127.0.0.1:2541","status":"Joining"},
		{"node":"pekko://X@127.0.0.1:2551","status":"Up"}],
	  "unreachable":[]}`
	st, err := parseMembersJSON([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if st.upCount != 1 {
		t.Fatalf("upCount=%d want 1", st.upCount)
	}
	if len(st.nonUp) != 1 || st.nonUp[0].status != "Joining" {
		t.Fatalf("expected 1 Joining peer, got %+v", st.nonUp)
	}
}
