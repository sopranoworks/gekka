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

// Pekko's real /cluster/members reports unreachable entries as objects
// ({"node": ..., "observedBy": [...]}), not bare strings.
func TestParseMembersResponse_PekkoUnreachableObjects(t *testing.T) {
	body := `{"selfNode":"pekko://X@127.0.0.1:2551","members":[
		{"node":"pekko://X@127.0.0.1:2551","nodeUid":"1","status":"Up","roles":["dc-default"]},
		{"node":"pekko://X@127.0.0.1:2541","nodeUid":"2","status":"Up","roles":["dc-default"]}],
	  "unreachable":[{"node":"pekko://X@127.0.0.1:2541","observedBy":["pekko://X@127.0.0.1:2551"]}],
	  "leader":"pekko://X@127.0.0.1:2541","oldest":"pekko://X@127.0.0.1:2551"}`
	st, err := parseMembersJSON([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if st.upCount != 2 {
		t.Fatalf("upCount=%d want 2", st.upCount)
	}
	if len(st.unreachable) != 1 || st.unreachable[0] != "pekko://X@127.0.0.1:2541" {
		t.Fatalf("expected the unreachable node's address, got %v", st.unreachable)
	}
}

// gekka's management endpoint returns a bare JSON ARRAY of member objects
// with "address"/"status"/"reachable" fields (internal/management/server.go
// MemberInfo) — the parser must understand that shape too.
func TestParseMembersResponse_GekkaArrayShape(t *testing.T) {
	body := `[
		{"address":"pekko://X@127.0.0.1:2541","status":"Up","roles":["dc-default"],"dc":"default","upNumber":6,"reachable":true,"latency_ms":0,"self":true},
		{"address":"pekko://X@127.0.0.1:2551","status":"Up","roles":["dc-default"],"dc":"default","upNumber":1,"reachable":true,"latency_ms":0,"self":false}
	]`
	st, err := parseMembersJSON([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if st.upCount != 2 {
		t.Fatalf("upCount=%d want 2", st.upCount)
	}
	if len(st.nonUp) != 0 || len(st.unreachable) != 0 {
		t.Fatalf("want fully-Up reachable view, got nonUp=%v unreachable=%v", st.nonUp, st.unreachable)
	}
}

func TestParseMembersResponse_GekkaArrayShape_NonUpAndUnreachable(t *testing.T) {
	body := `[
		{"address":"pekko://X@127.0.0.1:2541","status":"Up","reachable":true},
		{"address":"pekko://X@127.0.0.1:2542","status":"Joining","reachable":true},
		{"address":"pekko://X@127.0.0.1:2551","status":"Up","reachable":false}
	]`
	st, err := parseMembersJSON([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if st.upCount != 2 {
		t.Fatalf("upCount=%d want 2", st.upCount)
	}
	if len(st.nonUp) != 1 || st.nonUp[0].status != "Joining" {
		t.Fatalf("expected 1 Joining peer, got %+v", st.nonUp)
	}
	if len(st.unreachable) != 1 || st.unreachable[0] != "pekko://X@127.0.0.1:2551" {
		t.Fatalf("expected the unreachable member's address, got %v", st.unreachable)
	}
}
