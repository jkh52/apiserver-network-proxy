/*
Copyright 2024 The Kubernetes Authors.

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
	"context"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"
	commonmetrics "sigs.k8s.io/apiserver-network-proxy/konnectivity-client/pkg/common/metrics"
	"sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
	"sigs.k8s.io/apiserver-network-proxy/pkg/server/metrics"
	"sigs.k8s.io/apiserver-network-proxy/proto/agent"
	"sigs.k8s.io/apiserver-network-proxy/proto/header"
	// "k8s.io/klog/v2"
)

// Backend abstracts a connected Konnectivity agent stream.
//
// In the only currently supported case (gRPC), it wraps an
// agent.AgentService_ConnectServer, provides synchronization and
// emits common stream metrics.
type Backend interface {
	Send(p *client.Packet) error
	Recv() (*client.Packet, error)
	Context() context.Context
	GetAgentID() string
	GetAgentIdentifiers() header.Identifiers
}

var _ Backend = &backend{}

type backend struct {
	sendLock sync.Mutex
	recvLock sync.Mutex
	conn     agent.AgentService_ConnectServer

	// cached from conn.Context()
	id     string
	idents header.Identifiers
}

func (b *backend) Send(p *client.Packet) error {
	b.sendLock.Lock()
	defer b.sendLock.Unlock()

	const segment = commonmetrics.SegmentToAgent
	metrics.Metrics.ObservePacket(segment, p.Type)
	err := b.conn.Send(p)
	if err != nil && err != io.EOF {
		metrics.Metrics.ObserveStreamError(segment, err, p.Type)
	}
	return err
}

func (b *backend) Recv() (*client.Packet, error) {
	b.recvLock.Lock()
	defer b.recvLock.Unlock()

	const segment = commonmetrics.SegmentFromAgent
	pkt, err := b.conn.Recv()
	if err != nil {
		if err != io.EOF {
			metrics.Metrics.ObserveStreamErrorNoPacket(segment, err)
		}
		return nil, err
	}
	metrics.Metrics.ObservePacket(segment, pkt.Type)
	return pkt, nil
}

func (b *backend) Context() context.Context {
	return b.conn.Context()
}

func (b *backend) GetAgentID() string {
	return b.id
}

func (b *backend) GetAgentIdentifiers() header.Identifiers {
	return b.idents
}

func getAgentID(stream agent.AgentService_ConnectServer) (string, error) {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return "", fmt.Errorf("failed to get context")
	}
	agentIDs := md.Get(header.AgentID)
	if len(agentIDs) != 1 {
		return "", fmt.Errorf("expected one agent ID in the context, got %v", agentIDs)
	}
	return agentIDs[0], nil
}

func getAgentIdentifiers(conn agent.AgentService_ConnectServer) (header.Identifiers, error) {
	var agentIdentifiers header.Identifiers
	md, ok := metadata.FromIncomingContext(conn.Context())
	if !ok {
		return agentIdentifiers, fmt.Errorf("failed to get metadata from context")
	}
	agentIdent := md.Get(header.AgentIdentifiers)
	if len(agentIdent) > 1 {
		return agentIdentifiers, fmt.Errorf("expected at most one set of agent identifiers in the context, got %v", agentIdent)
	}
	if len(agentIdent) == 0 {
		return agentIdentifiers, nil
	}

	return header.GenAgentIdentifiers(agentIdent[0])
}

func NewBackend(conn agent.AgentService_ConnectServer) (Backend, error) {
	agentID, err := getAgentID(conn)
	if err != nil {
		return nil, err
	}
	agentIdentifiers, err := getAgentIdentifiers(conn)
	if err != nil {
		return nil, err
	}
	return &backend{conn: conn, id: agentID, idents: agentIdentifiers}, nil
}

// BackendStorage is an interface for an in-memory storage of backend
// connections.
//
// A key may be associated with multiple Backend objects. For example,
// a given agent can have multiple re-connects in flight, and multiple
// agents could share a common host identifier.
type BackendStorage interface {
	// AddBackend registers a backend, and returns the new number of backends in this storage.
	AddBackend(keys []string, backend Backend) int
	// RemoveBackend removes a backend, and returns the new number of backends in this storage.
	RemoveBackend(keys []string, backend Backend) int
	// SetDraining marks a backend as draining.
	SetDraining(keys []string, backend Backend)
	// Backend selects a backend by key.
	Backend(key string) (Backend, error)
	// RandomBackend selects a random backend.
	RandomBackend() (Backend, error)
	// NumKeys returns the distinct count of backend keys in this storage.
	NumKeys() int
}

// DefaultBackendStorage is the default BackendStorage
type DefaultBackendStorage struct {
	mu sync.RWMutex //protects the following
	// A map from key to grpc connections.
	// For a given "backends []Backend", ProxyServer prefers backends[0] to send
	// traffic, because backends[1:] are more likely to be closed
	// by the agent to deduplicate connections to the same server.
	backends map[string][]Backend
	// Cache of backends keys, to efficiently select random.
	backendKeys []string

	// Agents in draining state are moved from "backends" to here.
	draining map[string][]Backend

	random *rand.Rand
}

var _ BackendStorage = &DefaultBackendStorage{}

// NewDefaultBackendStorage returns a DefaultBackendStorage
func NewDefaultBackendStorage() *DefaultBackendStorage {
	// Set an explicit value, so that the metric is emitted even when
	// no agent ever successfully connects.
	return &DefaultBackendStorage{
		backends: make(map[string][]Backend),
		random:   rand.New(rand.NewSource(time.Now().UnixNano())),
	} /* #nosec G404 */
}

func (s *DefaultBackendStorage) AddBackend(keys []string, backend Backend) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		if key == "" {
			continue
		}
		_, ok := s.backends[key]
		if ok {
			if !slices.Contains(s.backends[key], backend) {
				s.backends[key] = append(s.backends[key], backend)
			}
			continue
		}
		s.backends[key] = []Backend{backend}
		s.backendKeys = append(s.backendKeys, key)
	}
	return len(s.backends)
}

func (s *DefaultBackendStorage) RemoveBackend(keys []string, backend Backend) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		if key == "" {
			continue
		}
		backends, ok := s.backends[key]
		if !ok {
			continue
		}
		for i, b := range backends {
			if b == backend {
				s.backends[key] = slices.Delete(backends, i, i+1)
			}
		}
		if len(s.backends[key]) == 0 {
			delete(s.backends, key)
			s.backendKeys = slices.DeleteFunc(s.backendKeys, func(k string) bool {
				return k == key
			})
		}
	}
	return len(s.backends)
}

func (s *DefaultBackendStorage) SetDraining(keys []string, backend Backend) {
	// DefaultBackendStorage is not draining-aware.
}

func (s *DefaultBackendStorage) Backend(key string) (Backend, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bes, exist := s.backends[key]
	if exist && len(bes) > 0 {
		// always return the first connection to an agent, because the agent
		// will close later connections if there are multiple.
		return bes[0], nil
	}
	return nil, &ErrNotFound{}
}

func (s *DefaultBackendStorage) RandomBackend() (Backend, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.backends) == 0 {
		return nil, &ErrNotFound{}
	}
	key := s.backendKeys[s.random.Intn(len(s.backendKeys))]
	// always return the first connection to an agent, because the agent
	// will close later connections if there are multiple.
	return s.backends[key][0], nil
}

func (s *DefaultBackendStorage) NumKeys() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.backends)
}

// DrainingBackendStorage is a BackendStorage that is aware of agent
// draining state and prefers non-draining agents.
type DrainingBackendStorage struct {
	// Protects write funcions. In particular, avoid
	// AddBackend() racing with SetDraining() for a given Backend.
	mu sync.RWMutex

	primary  *DefaultBackendStorage
	draining *DefaultBackendStorage
}

func NewDrainingBackendStorage() *DrainingBackendStorage {
	return &DrainingBackendStorage{
		primary:  NewDefaultBackendStorage(),
		draining: NewDefaultBackendStorage(),
	}
}

func (s *DrainingBackendStorage) AddBackend(keys []string, backend Backend) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.primary.AddBackend(keys, backend)
}

func (s *DrainingBackendStorage) RemoveBackend(keys []string, backend Backend) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	p := s.primary.RemoveBackend(keys, backend)
	d := s.draining.RemoveBackend(keys, backend)
	return p + d
}

func (s *DrainingBackendStorage) SetDraining(keys []string, backend Backend) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.primary.RemoveBackend(keys, backend)
	s.draining.AddBackend(keys, backend)
}

func (s *DrainingBackendStorage) Backend(key string) (Backend, error) {
	b, e := s.primary.Backend(key)
	if _, ok := e.(*ErrNotFound); ok {
		return s.draining.Backend(key)
	}
	return b, e
}

func (s *DrainingBackendStorage) RandomBackend() (Backend, error) {
	b, e := s.primary.RandomBackend()
	if _, ok := e.(*ErrNotFound); ok {
		return s.draining.RandomBackend()
	}
	return b, e
}

func (s *DrainingBackendStorage) NumKeys() int {
	p := s.primary.NumKeys()
	d := s.draining.NumKeys()
	return p + d
}
