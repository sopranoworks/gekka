package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ─── Mock SeedProvider ───────────────────────────────────────────────────────

type mockSeedProvider struct {
	mu    sync.Mutex
	nodes []string
	err   error
}

func (m *mockSeedProvider) FetchSeedNodes() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	cp := make([]string, len(m.nodes))
	copy(cp, m.nodes)
	return cp, nil
}

func (m *mockSeedProvider) setNodes(nodes []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = nodes
}

// ─── Tests ───────────────────────────────────────────────────────────────────

func TestClusterBootstrap_QuorumLogic(t *testing.T) {
	provider := &mockSeedProvider{
		nodes: []string{"10.0.0.1:2551", "10.0.0.2:2551", "10.0.0.3:2551"},
	}

	var joinedHost string
	var joinedPort uint32
	var joinCount atomic.Int32

	config := BootstrapConfig{
		RequiredContactPoints: 3,
		StableMargin:          50 * time.Millisecond,
		DiscoveryInterval:     10 * time.Millisecond,
	}
	bootstrap := NewClusterBootstrap(provider, config)
	bootstrap.joinFn = func(ctx context.Context, host string, port uint32) error {
		joinedHost = host
		joinedPort = port
		joinCount.Add(1)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	bootstrap.Start(ctx, nil)

	// Wait for bootstrap to complete.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("bootstrap did not complete within timeout")
		default:
		}
		if joinCount.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	bootstrap.Stop()

	if joinCount.Load() != 1 {
		t.Fatalf("expected 1 join call, got %d", joinCount.Load())
	}

	// Should join to lowest address: 10.0.0.1:2551
	if joinedHost != "10.0.0.1" || joinedPort != 2551 {
		t.Errorf("expected join to 10.0.0.1:2551, got %s:%d", joinedHost, joinedPort)
	}
}

func TestClusterBootstrap_LeaderElectionByAddress(t *testing.T) {
	// Leader should be the lexicographically lowest address.
	leader := ResolveLeader([]string{"10.0.0.3:2551", "10.0.0.1:2551", "10.0.0.2:2551"})
	if leader != "10.0.0.1:2551" {
		t.Errorf("expected leader 10.0.0.1:2551, got %s", leader)
	}

	leader = ResolveLeader([]string{"b.example.com:2551", "a.example.com:2551"})
	if leader != "a.example.com:2551" {
		t.Errorf("expected leader a.example.com:2551, got %s", leader)
	}

	leader = ResolveLeader([]string{})
	if leader != "" {
		t.Errorf("expected empty leader for empty contacts, got %s", leader)
	}
}

func TestClusterBootstrap_StableMarginWait(t *testing.T) {
	provider := &mockSeedProvider{
		nodes: []string{"10.0.0.1:2551", "10.0.0.2:2551"},
	}

	var joinCount atomic.Int32
	stableMargin := 100 * time.Millisecond

	config := BootstrapConfig{
		RequiredContactPoints: 2,
		StableMargin:          stableMargin,
		DiscoveryInterval:     10 * time.Millisecond,
	}
	bootstrap := NewClusterBootstrap(provider, config)
	bootstrap.joinFn = func(ctx context.Context, host string, port uint32) error {
		joinCount.Add(1)
		return nil
	}

	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	bootstrap.Start(ctx, nil)

	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("bootstrap did not complete within timeout")
		default:
		}
		if joinCount.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	elapsed := time.Since(start)
	bootstrap.Stop()

	if elapsed < stableMargin {
		t.Errorf("bootstrap joined too quickly (%v), should wait at least %v", elapsed, stableMargin)
	}
}

func TestClusterBootstrap_ContactsChangeResetsStability(t *testing.T) {
	provider := &mockSeedProvider{
		nodes: []string{"10.0.0.1:2551", "10.0.0.2:2551"},
	}

	var joinCount atomic.Int32

	config := BootstrapConfig{
		RequiredContactPoints: 2,
		StableMargin:          80 * time.Millisecond,
		DiscoveryInterval:     10 * time.Millisecond,
	}
	bootstrap := NewClusterBootstrap(provider, config)
	bootstrap.joinFn = func(ctx context.Context, host string, port uint32) error {
		joinCount.Add(1)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	bootstrap.Start(ctx, nil)

	// After 40ms, change the contact list (should reset stability timer).
	time.Sleep(40 * time.Millisecond)
	provider.setNodes([]string{"10.0.0.1:2551", "10.0.0.3:2551"})

	// Wait a bit — should not have joined yet because stability was reset.
	time.Sleep(50 * time.Millisecond)
	if joinCount.Load() > 0 {
		t.Fatal("should not have joined yet after contact change")
	}

	// Eventually should join after stable margin passes.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("bootstrap did not complete within timeout")
		default:
		}
		if joinCount.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	bootstrap.Stop()
}

func TestClusterBootstrap_InsufficientQuorum(t *testing.T) {
	provider := &mockSeedProvider{
		nodes: []string{"10.0.0.1:2551"}, // only 1, need 2
	}

	var joinCount atomic.Int32

	config := BootstrapConfig{
		RequiredContactPoints: 2,
		StableMargin:          20 * time.Millisecond,
		DiscoveryInterval:     10 * time.Millisecond,
	}
	bootstrap := NewClusterBootstrap(provider, config)
	bootstrap.joinFn = func(ctx context.Context, host string, port uint32) error {
		joinCount.Add(1)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	bootstrap.Start(ctx, nil)

	<-ctx.Done()
	bootstrap.Stop()

	if joinCount.Load() != 0 {
		t.Fatal("should not join when quorum is not met")
	}
}

func TestClusterBootstrap_StopBeforeJoin(t *testing.T) {
	provider := &mockSeedProvider{
		nodes: []string{"10.0.0.1:2551", "10.0.0.2:2551"},
	}

	var joinCount atomic.Int32

	config := BootstrapConfig{
		RequiredContactPoints: 2,
		StableMargin:          5 * time.Second, // very long — won't be reached
		DiscoveryInterval:     10 * time.Millisecond,
	}
	bootstrap := NewClusterBootstrap(provider, config)
	bootstrap.joinFn = func(ctx context.Context, host string, port uint32) error {
		joinCount.Add(1)
		return nil
	}

	ctx := context.Background()
	bootstrap.Start(ctx, nil)

	// Stop before stable margin is reached.
	time.Sleep(50 * time.Millisecond)
	bootstrap.Stop()

	if joinCount.Load() != 0 {
		t.Fatal("should not join when stopped before stable margin")
	}

	if bootstrap.IsRunning() {
		t.Fatal("should not be running after Stop")
	}
}

func TestNormalizeAddresses(t *testing.T) {
	result := normalizeAddresses([]string{"  a:1  ", "b:2", "", "a:1", "c:3"})
	if len(result) != 3 {
		t.Fatalf("expected 3 unique addresses, got %d: %v", len(result), result)
	}
}

func TestParseHostPort(t *testing.T) {
	host, port, err := parseHostPort("10.0.0.1:2551")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "10.0.0.1" || port != 2551 {
		t.Errorf("expected 10.0.0.1:2551, got %s:%d", host, port)
	}

	_, _, err = parseHostPort("invalid")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}
