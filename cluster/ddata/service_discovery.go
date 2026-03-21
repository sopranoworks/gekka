/*
 * service_discovery.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

// ServiceDiscovery provides eventually-consistent distributed service
// registration and lookup backed by the Replicator's ORSet CRDT.
//
// Each service name maps to an independent ORSet stored in the Replicator.
// Registering an address adds it to the set with the local node's identity;
// unregistering removes all observed dots for that address (standard OR-Set
// remove semantics).
//
// Because ORSet uses add-wins on concurrent add/remove from different nodes,
// an address that is re-registered concurrently with its removal on another
// node will survive the merge.  This is the desired behaviour for service
// discovery: prefer availability over consistency.
//
// Gossip is handled entirely by the underlying Replicator.  Call
// Replicator.Start to enable periodic background sync.
//
// Usage:
//
//	r  := ddata.NewReplicator("node-1", router)
//	sd := ddata.NewServiceDiscovery(r)
//	sd.RegisterService("order-processor", "10.0.0.1:8080")
//	addrs := sd.LookupService("order-processor")
type ServiceDiscovery struct {
	r *Replicator
}

// NewServiceDiscovery returns a ServiceDiscovery backed by the given Replicator.
func NewServiceDiscovery(r *Replicator) *ServiceDiscovery {
	return &ServiceDiscovery{r: r}
}

// RegisterService adds address to the live address set for serviceName.
// The entry is tagged with the Replicator's nodeID so concurrent registrations
// from different nodes are preserved during merge.
// The change propagates to peers during the next gossip round.
func (s *ServiceDiscovery) RegisterService(serviceName, address string) {
	s.r.AddToSet(serviceName, address, WriteLocal)
}

// UnregisterService removes address from the live address set for serviceName.
// Only the locally-observed dots are removed; dots added concurrently on other
// nodes survive the merge (add-wins semantics).
func (s *ServiceDiscovery) UnregisterService(serviceName, address string) {
	s.r.RemoveFromSet(serviceName, address, WriteLocal)
}

// LookupService returns all addresses currently registered for serviceName on
// this node, including those received via gossip from other nodes.
// Returns nil when no addresses are known.
func (s *ServiceDiscovery) LookupService(serviceName string) []string {
	return s.r.ORSet(serviceName).Elements()
}

// RegisteredServices returns the names of all services that have at least one
// known address on this node.
func (s *ServiceDiscovery) RegisteredServices() []string {
	s.r.mu.RLock()
	defer s.r.mu.RUnlock()
	names := make([]string, 0, len(s.r.sets))
	for name, set := range s.r.sets {
		if len(set.Elements()) > 0 {
			names = append(names, name)
		}
	}
	return names
}
