/*
Copyright 2019 The Kubernetes Authors.

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

package header

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"

	"k8s.io/klog/v2"
)

const (
	ServerCount      = "serverCount"
	ServerID         = "serverID"
	AgentID          = "agentID"
	AgentIdentifiers = "agentIdentifiers"
	// AuthenticationTokenContextKey will be used as a key to store authentication tokens in grpc call
	// (https://tools.ietf.org/html/rfc6750#section-2.1)
	AuthenticationTokenContextKey = "Authorization"

	// AuthenticationTokenContextSchemePrefix has a prefix for auth token's content.
	// (https://tools.ietf.org/html/rfc6750#section-2.1)
	AuthenticationTokenContextSchemePrefix = "Bearer "

	// UserAgent is used to provide the client information in a proxy request
	UserAgent = "user-agent"
)

// Identifiers stores agent identifiers that will be used by the server when
// choosing agents
type Identifiers struct {
	IPv4         []string
	IPv6         []string
	Host         []string
	CIDR         []string
	DefaultRoute bool
}

type IdentifierType string

const (
	IPv4         IdentifierType = "ipv4"
	IPv6         IdentifierType = "ipv6"
	Host         IdentifierType = "host"
	CIDR         IdentifierType = "cidr"
	UID          IdentifierType = "uid"
	DefaultRoute IdentifierType = "default-route"
)

// ValidateAgentIdentifiers returns an error if the URL encoded agent
// identifers are invalid or unrecognized. This is intended to be used
// by konnectivity-agent, to validate what it sends
// konnectivity-server.
func ValidateAgentIdentifiers(addrs string) error {
	var errs []error
	decoded, err := url.ParseQuery(addrs)
	if err != nil {
		return fmt.Errorf("fail to parse url encoded string: %w", err)
	}
	for idType, ids := range decoded {
		switch IdentifierType(idType) {
		case IPv4:
			for _, id := range ids {
				ip := net.ParseIP(id)
				if ip == nil {
					errs = append(errs, fmt.Errorf("invalid IPv4: %v", id))
				}
			}
		case IPv6:
			for _, id := range ids {
				ip := net.ParseIP(id)
				if ip == nil {
					errs = append(errs, fmt.Errorf("invalid IPv6: %v", id))
				}
			}
		case Host:
			// TODO: validate
			continue
		case CIDR:
			for _, id := range ids {
				_, _, err := net.ParseCIDR(id)
				if err != nil {
					errs = append(errs, fmt.Errorf("invalid cidr: %w", err))
				}
			}
			continue
		case DefaultRoute:
			if len(ids) != 1 {
				errs = append(errs, fmt.Errorf("wrong number of default-route entries: %v", ids))
			}
			for _, id := range ids {
				_, err := strconv.ParseBool(id)
				if err != nil {
					errs = append(errs, fmt.Errorf("invalid default-route %q: %w", id, err))
				}
			}
		default:
			errs = append(errs, fmt.Errorf("unknown identifier type: %v", idType))
		}
	}
	return errors.Join(errs...)
}

// GenAgentIdentifiers generates an Identifiers based on the input string, the
// input string should be a comma-separated list with each item in the format
// of <IdentifierType>=<address>
func GenAgentIdentifiers(addrs string) (Identifiers, error) {
	var agentIdents Identifiers
	decoded, err := url.ParseQuery(addrs)
	if err != nil {
		return agentIdents, fmt.Errorf("fail to parse url encoded string: %v", err)
	}
	for idType, ids := range decoded {
		switch IdentifierType(idType) {
		case IPv4:
			agentIdents.IPv4 = append(agentIdents.IPv4, ids...)
		case IPv6:
			agentIdents.IPv6 = append(agentIdents.IPv6, ids...)
		case Host:
			agentIdents.Host = append(agentIdents.Host, ids...)
		case CIDR:
			agentIdents.CIDR = append(agentIdents.CIDR, ids...)
		case DefaultRoute:
			defaultRouteIdentifier, err := strconv.ParseBool(ids[0])
			if err == nil && defaultRouteIdentifier {
				agentIdents.DefaultRoute = true
			}
		default:
			// To support binary skew with agents that send new identifier type,
			// fail open. The better place to validate more strictly is within the agent.
			klog.Warningf("Unrecognized agent identifier type: %v", idType)
			continue
		}
	}
	return agentIdents, nil
}
