/*
 * deployment_config.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"fmt"
	"strings"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"

	hocon "github.com/sopranoworks/gekka-config"
)

// ClusterRouterSettings holds the cluster-aware routing configuration.
type ClusterRouterSettings struct {
	// Enabled reports whether cluster-aware routing is active.
	Enabled bool `hocon:"enabled"`

	// AllowLocalRoutees allows the router to include routees on the local node.
	AllowLocalRoutees bool `hocon:"allow-local-routees"`

	// UseRole restricts routing to nodes with a specific role.
	UseRole string `hocon:"use-role"`

	// TotalInstances is the target total number of routees across the entire cluster.
	TotalInstances int `hocon:"total-instances"`
}

// DeploymentConfig holds the router deployment settings parsed from the
// akka.actor.deployment (or pekko.actor.deployment) HOCON block.
//
// Typical HOCON source:
//
//	pekko.actor.deployment {
//	  "/user/myRouter" {
//	    router         = round-robin-pool
//	    nr-of-instances = 5
//	    cluster {
//	      enabled = on
//	      allow-local-routees = on
//	      use-role = "backend"
//	      total-instances = 10
//	    }
//	  }
//	}
type DeploymentConfig struct {
	// Router is the router-type identifier, e.g. "round-robin-pool".
	// An empty string means no router is configured (plain actor).
	Router string `hocon:"router"`

	// NrOfInstances is the pool size for local-only pool routers.
	// For cluster-aware pools, use TotalInstances.
	NrOfInstances int `hocon:"nr-of-instances"`

	// RouteesPaths holds the explicit routee paths for group routers.
	RouteesPaths []string `hocon:"routees.paths"`

	// VirtualNodesFactor is the number of tokens per routee on the hash ring.
	VirtualNodesFactor int `hocon:"virtual-nodes-factor"`

	// Cluster holds settings for cluster-aware routing.
	Cluster ClusterRouterSettings `hocon:"cluster"`
}

// LookupDeployment finds the deployment block for actorPath inside a parsed
// HOCON config and returns the corresponding DeploymentConfig.
//
// actorPath may be the full user path ("/user/myRouter") or the short form
// ("/myRouter"). Both are tried against the deployment section. Both pekko.*
// and akka.* top-level prefixes are searched.
//
// The HOCON deployment key must be a quoted string (standard Pekko convention):
//
//	pekko.actor.deployment {
//	  "/user/myRouter" { ... }  // full path — preferred
//	  "/myRouter"      { ... }  // short form — also accepted
//	}
//
// Returns (config, true) when a matching block is found; (zero, false) when
// no deployment block exists for actorPath (treat the actor as a plain actor).
func LookupDeployment(cfg *hocon.Config, actorPath string) (DeploymentConfig, bool) {
	for _, prefix := range []string{"pekko", "akka"} {
		if dc, ok := lookupDeploymentUnder(cfg, prefix, actorPath); ok {
			return dc, true
		}
	}
	return DeploymentConfig{}, false
}

// lookupDeploymentUnder searches prefix.actor.deployment for actorPath.
func lookupDeploymentUnder(cfg *hocon.Config, prefix, actorPath string) (DeploymentConfig, bool) {
	depCfg, err := cfg.GetConfig(prefix + ".actor.deployment")
	if err != nil {
		return DeploymentConfig{}, false
	}

	for _, key := range deploymentKeyCandidates(actorPath) {
		actorCfg, err := depCfg.GetConfig(key)
		if err == nil {
			return parseDeploymentObject(actorCfg), true
		}
	}
	return DeploymentConfig{}, false
}

// deploymentKeyCandidates returns the HOCON field names to try for actorPath,
// supporting both the full path and the /user-relative short form.
func deploymentKeyCandidates(actorPath string) []string {
	candidates := []string{actorPath}
	switch {
	case strings.HasPrefix(actorPath, "/user/"):
		// "/user/myRouter" → also try "/myRouter"
		candidates = append(candidates, "/"+strings.TrimPrefix(actorPath, "/user/"))
	case strings.HasPrefix(actorPath, "/") && actorPath != "/user":
		// "/myRouter" → also try "/user/myRouter"
		candidates = append(candidates, "/user"+actorPath)
	}
	return candidates
}

// parseDeploymentObject extracts DeploymentConfig fields from the sub-config
// rooted at a single deployment block object.
func parseDeploymentObject(cfg hocon.Config) DeploymentConfig {
	var dc DeploymentConfig

	if r, err := cfg.GetString("router"); err == nil {
		dc.Router = strings.Trim(r, `"`)
	}
	if n, err := cfg.GetInt("nr-of-instances"); err == nil {
		dc.NrOfInstances = n
	}

	var tmp struct {
		Paths []string `hocon:"routees.paths"`
	}
	_ = cfg.Unmarshal(&tmp)
	dc.RouteesPaths = append(dc.RouteesPaths, tmp.Paths...)

	if f, err := cfg.GetInt("virtual-nodes-factor"); err == nil {
		dc.VirtualNodesFactor = f
	}

	// cluster block settings
	if c, err := cfg.GetConfig("cluster"); err == nil {
		if enabled, err := c.GetBoolean("enabled"); err == nil {
			dc.Cluster.Enabled = enabled
		}
		if allow, err := c.GetBoolean("allow-local-routees"); err == nil {
			dc.Cluster.AllowLocalRoutees = allow
		}
		if role, err := c.GetString("use-role"); err == nil {
			dc.Cluster.UseRole = strings.Trim(role, `"`)
		}
		if total, err := c.GetInt("total-instances"); err == nil {
			dc.Cluster.TotalInstances = total
		}
	}

	return dc
}

// ── Pool router factory ───────────────────────────────────────────────────────

// isGroupRouter reports whether routerType identifies a group router
// (routes to pre-existing actors) rather than a pool router (owns routees).
// By convention, group router types end in "-group".
func isGroupRouter(routerType string) bool {
	return strings.HasSuffix(routerType, "-group")
}

// DeploymentToPoolRouter maps a DeploymentConfig to a ready-to-use PoolRouter.
//
// The router type string is mapped to the corresponding RoutingLogic:
//
//	"round-robin-pool"  →  *actor.RoundRobinRoutingLogic
//
// Returns an error if Router is not a recognised pool-router type or if
// NrOfInstances is zero (use the returned PoolRouter's AdjustPoolSize message
// to resize at runtime instead).
func DeploymentToPoolRouter(cm *cluster.ClusterManager, d DeploymentConfig, props actor.Props) (actor.Actor, error) {
	if !d.Cluster.Enabled && d.NrOfInstances <= 0 {
		return nil, fmt.Errorf("deployment: nr-of-instances must be > 0, got %d", d.NrOfInstances)
	}
	logic, err := routingLogicFor(d.Router)
	if err != nil {
		return nil, err
	}
	// Inject router-specific settings.
	if chl, ok := logic.(*actor.ConsistentHashRoutingLogic); ok {
		chl.VirtualNodesFactor = d.VirtualNodesFactor
	}

	if d.Cluster.Enabled {
		return cluster.NewClusterPoolRouter(cm, logic, d.Cluster.TotalInstances, d.Cluster.AllowLocalRoutees, d.Cluster.UseRole, props), nil
	}
	return actor.NewPoolRouter(logic, d.NrOfInstances, props), nil
}

// routingLogicFor maps a HOCON router-type string to the corresponding RoutingLogic.
// Both pool and group variants of the same distribution strategy use the same logic.
func routingLogicFor(routerType string) (actor.RoutingLogic, error) {
	switch routerType {
	case "round-robin-pool", "round-robin-group":
		return &actor.RoundRobinRoutingLogic{}, nil
	case "random-pool", "random-group":
		return &actor.RandomRoutingLogic{}, nil
	case "consistent-hashing-pool", "consistent-hashing-group":
		return &actor.ConsistentHashRoutingLogic{}, nil
	default:
		return nil, fmt.Errorf(
			"deployment: unknown router type %q (supported: round-robin-pool, round-robin-group, random-pool, random-group, consistent-hashing-pool, consistent-hashing-group)",
			routerType,
		)
	}
}

// DeploymentToGroupRouter maps a DeploymentConfig to a GroupRouter that will
// resolve its routee paths during PreStart.
//
// d.RouteesPaths must not be empty; use NewGroupRouter with explicit Ref values
// when routees are known at construction time.
func DeploymentToGroupRouter(cm *cluster.ClusterManager, d DeploymentConfig) (actor.Actor, error) {
	if !d.Cluster.Enabled && len(d.RouteesPaths) == 0 {
		return nil, fmt.Errorf("deployment: group router requires non-empty routees.paths or cluster enabled")
	}
	logic, err := routingLogicFor(d.Router)
	if err != nil {
		return nil, err
	}
	// Inject router-specific settings.
	if chl, ok := logic.(*actor.ConsistentHashRoutingLogic); ok {
		chl.VirtualNodesFactor = d.VirtualNodesFactor
	}

	if d.Cluster.Enabled {
		// Cluster group router typically resolves its own relative path on remote nodes.
		return cluster.NewClusterGroupRouter(cm, logic, d.RouteesPaths, d.Cluster.AllowLocalRoutees, d.Cluster.UseRole), nil
	}
	return actor.NewGroupRouterWithPaths(logic, d.RouteesPaths), nil
}

// ── Bulk extraction ───────────────────────────────────────────────────────────

// extractDeployments reads every entry under <prefix>.actor.deployment in cfg
// (trying both "pekko" and "akka" prefixes) and returns a map from actor path
// to DeploymentConfig. Only entries that have a non-empty Router field are
// included. Each entry is stored under both the key as written in the HOCON
// and its canonical counterpart (full path ↔ short form without /user).
func extractDeployments(cfg *hocon.Config) map[string]DeploymentConfig {
	result := make(map[string]DeploymentConfig)
	for _, prefix := range []string{"pekko", "akka"} {
		depCfg, err := cfg.GetConfig(prefix + ".actor.deployment")
		if err != nil {
			continue
		}
		var m map[string]DeploymentConfig
		if err := depCfg.Unmarshal(&m); err == nil && m != nil {
			for k, v := range m {
				result[k] = v
				// Also store under the alternate form (full ↔ short)
				for _, alt := range deploymentKeyCandidates(k)[1:] {
					if _, exists := result[alt]; !exists {
						result[alt] = v
					}
				}
			}
		}
	}
	return result
}
