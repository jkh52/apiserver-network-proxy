/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"fmt"
	"slices"
	"strings"

	"sigs.k8s.io/apiserver-network-proxy/pkg/server/metrics"
)

type ProxyStrategy int

const (
	// With this strategy the Proxy Server will randomly pick a backend from
	// the current healthy backends to establish the tunnel over which to
	// forward requests.
	ProxyStrategyDefault ProxyStrategy = iota + 1
	// With this strategy the Proxy Server will pick a backend that has the same
	// associated host as the request.Host to establish the tunnel.
	ProxyStrategyDestHost
	// ProxyStrategyDefaultRoute will only forward traffic to agents that have explicity advertised
	// they serve the default route through an agent identifier. Typically used in combination with destHost
	ProxyStrategyDefaultRoute
)

func (ps ProxyStrategy) String() string {
	switch ps {
	case ProxyStrategyDefault:
		return "default"
	case ProxyStrategyDestHost:
		return "destHost"
	case ProxyStrategyDefaultRoute:
		return "defaultRoute"
	}
	panic("unhandled ProxyStrategy String()")
}

func ParseProxyStrategy(s string) (ProxyStrategy, error) {
	switch s {
	case ProxyStrategyDefault.String():
		return ProxyStrategyDefault, nil
	case ProxyStrategyDestHost.String():
		return ProxyStrategyDestHost, nil
	case ProxyStrategyDefaultRoute.String():
		return ProxyStrategyDefaultRoute, nil
	default:
		return 0, fmt.Errorf("unknown proxy strategy: %s", s)
	}
}

// GenProxyStrategiesFromStr generates the list of proxy strategies from the
// comma-seperated string, i.e., destHost.
func ParseProxyStrategies(proxyStrategies string) ([]ProxyStrategy, error) {
	var result []ProxyStrategy

	strs := strings.Split(proxyStrategies, ",")
	for _, s := range strs {
		if len(s) == 0 {
			continue
		}
		ps, err := ParseProxyStrategy(s)
		if err != nil {
			return nil, err
		}
		result = append(result, ps)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("proxy strategies cannot be empty")
	}
	return result, nil
}

// BackendManager is an interface to manage backend connections, i.e.,
// connection to the proxy agents.
type BackendManager interface {
	// Backend returns a backend connection according to proxy strategies.
	Backend(addr string) (Backend, error)
	// AddBackend adds a backend.
	AddBackend(backend Backend)
	// RemoveBackend adds a backend.
	RemoveBackend(backend Backend)
	// SetDraining marks a backend as draining.
	SetDraining(backend Backend)
	// NumBackends returns the number of backends.
	NumBackends() int
	ReadinessManager
}

type DefaultBackendManager struct {
	proxyStrategies []ProxyStrategy

	// All backends by agentID.
	all BackendStorage
	// All backends by host identifier(s). Only used with ProxyStrategyDestHost.
	byHost BackendStorage
	// All default-route backends, by agentID. Only used with ProxyStrategyDefaultRoute.
	byDefaultRoute BackendStorage
}

var _ BackendManager = &DefaultBackendManager{}

// NewDefaultBackendManager returns a DefaultBackendStorage
func NewDefaultBackendManager(proxyStrategies []ProxyStrategy) *DefaultBackendManager {
	metrics.Metrics.SetBackendCount(0)
	return &DefaultBackendManager{
		proxyStrategies: proxyStrategies,
		all:             NewDrainingBackendStorage(),
		byHost:          NewDrainingBackendStorage(),
		byDefaultRoute:  NewDrainingBackendStorage(),
	}
}

func (s *DefaultBackendManager) Backend(addr string) (Backend, error) {
	for _, strategy := range s.proxyStrategies {
		var b Backend
		var e error
		e = &ErrNotFound{}
		switch strategy {
		case ProxyStrategyDefault:
			b, e = s.all.RandomBackend()
		case ProxyStrategyDestHost:
			b, e = s.byHost.Backend(addr)
		case ProxyStrategyDefaultRoute:
			b, e = s.byDefaultRoute.RandomBackend()
		}
		if e == nil {
			return b, nil
		}
	}
	return nil, &ErrNotFound{}
}

func hostIdentifiers(backend Backend) []string {
	hosts := []string{}
	hosts = append(hosts, backend.GetAgentIdentifiers().IPv4...)
	hosts = append(hosts, backend.GetAgentIdentifiers().IPv6...)
	hosts = append(hosts, backend.GetAgentIdentifiers().Host...)
	return hosts
}

func (s *DefaultBackendManager) AddBackend(backend Backend) {
	agentID := backend.GetAgentID()
	count := s.all.AddBackend([]string{agentID}, backend)
	if slices.Contains(s.proxyStrategies, ProxyStrategyDestHost) {
		idents := hostIdentifiers(backend)
		s.byHost.AddBackend(idents, backend)
	}
	if slices.Contains(s.proxyStrategies, ProxyStrategyDefaultRoute) {
		if backend.GetAgentIdentifiers().DefaultRoute {
			s.byDefaultRoute.AddBackend([]string{agentID}, backend)
		}
	}
	metrics.Metrics.SetBackendCount(count)
}

func (s *DefaultBackendManager) RemoveBackend(backend Backend) {
	agentID := backend.GetAgentID()
	count := s.all.RemoveBackend([]string{agentID}, backend)
	if slices.Contains(s.proxyStrategies, ProxyStrategyDestHost) {
		idents := hostIdentifiers(backend)
		s.byHost.RemoveBackend(idents, backend)
	}
	if slices.Contains(s.proxyStrategies, ProxyStrategyDefaultRoute) {
		if backend.GetAgentIdentifiers().DefaultRoute {
			s.byDefaultRoute.RemoveBackend([]string{agentID}, backend)
		}
	}
	metrics.Metrics.SetBackendCount(count)
}

func (s *DefaultBackendManager) SetDraining(backend Backend) {
	agentID := backend.GetAgentID()
	s.all.SetDraining([]string{agentID}, backend)
	if slices.Contains(s.proxyStrategies, ProxyStrategyDestHost) {
		idents := hostIdentifiers(backend)
		s.byHost.SetDraining(idents, backend)
	}
	if slices.Contains(s.proxyStrategies, ProxyStrategyDefaultRoute) {
		if backend.GetAgentIdentifiers().DefaultRoute {
			s.byDefaultRoute.SetDraining([]string{agentID}, backend)
		}
	}
}

func (s *DefaultBackendManager) NumBackends() int {
	return s.all.NumKeys()
}

// ErrNotFound indicates that no backend can be found.
type ErrNotFound struct{}

// Error returns the error message.
func (e *ErrNotFound) Error() string {
	return "No agent available"
}

func (s *DefaultBackendManager) Ready() (bool, string) {
	if s.NumBackends() == 0 {
		return false, "no connection to any proxy agent"
	}
	return true, ""
}
