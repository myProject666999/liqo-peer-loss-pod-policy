// Copyright 2019-2026 The Liqo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package liqonodeprovider

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/liqotech/liqo/pkg/consts"
)

// TestPeerObservationAnnotation verifies that the peer observation is published as an explicitly stale annotation,
// while the last successful observation is preserved when the peer is disconnected.
func TestPeerObservationAnnotation(t *testing.T) {
	provider := &LiqoNodeProvider{node: &corev1.Node{}}

	// Connected: the observation reports the connection and records the last ready instant.
	provider.networkReady = true
	provider.recordPeerObservation()
	provider.recomputeNodeState()

	if got := provider.node.Annotations[consts.PeerObservationAnnotationKey]; got != consts.PeerObservationConnected {
		t.Errorf("PeerObservation = %q, want %q", got, consts.PeerObservationConnected)
	}
	if got := provider.node.Annotations[consts.PeerObservationStaleAnnotationKey]; got != "true" {
		t.Errorf("PeerObservationStale = %q, want %q", got, "true")
	}
	if _, ok := provider.node.Annotations[consts.PeerLastObservedReadyAnnotationKey]; !ok {
		t.Error("expected the last observed ready annotation to be set while connected")
	}

	// Disconnected: the observation flips, the last observed ready is preserved and the annotation stays stale.
	provider.networkReady = false
	provider.recordPeerObservation()
	provider.recomputeNodeState()

	if got := provider.node.Annotations[consts.PeerObservationAnnotationKey]; got != consts.PeerObservationDisconnected {
		t.Errorf("PeerObservation = %q, want %q", got, consts.PeerObservationDisconnected)
	}
	if _, ok := provider.node.Annotations[consts.PeerLastObservedReadyAnnotationKey]; !ok {
		t.Error("expected the last observed ready annotation to be preserved while disconnected")
	}
	if got := provider.node.Annotations[consts.PeerObservationStaleAnnotationKey]; got != "true" {
		t.Errorf("PeerObservationStale = %q, want %q", got, "true")
	}
}
