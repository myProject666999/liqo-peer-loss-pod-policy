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

package consts

// Peer loss Pod policy.
//
// A peer can become unreachable while the pods offloaded to it are still running. Liqo cannot prove
// from the consumer side whether those pods have actually stopped, hence the policy below is deliberately
// conservative: by default pods are only marked as remotely unavailable and retained. Eviction (or the
// forced deletion of already terminating pods) must be explicitly authorized.
const (
	// PeerLossPolicyAnnotationKey is the annotation used on a local offloaded pod to override the default
	// peer-loss policy resolved from its controller type.
	PeerLossPolicyAnnotationKey = "liqo.io/peer-loss-policy"
	// PeerLossPolicyRetain keeps the pod (and its remote counterpart) untouched, only marking it unavailable.
	PeerLossPolicyRetain = "Retain"
	// PeerLossPolicyEvict authorizes the eviction (or forced deletion of terminating pods) once the peer loss is confirmed.
	PeerLossPolicyEvict = "Evict"

	// PeerObservationAnnotationKey is the annotation reporting the last observed connectivity towards the peer cluster.
	PeerObservationAnnotationKey = "liqo.io/peer-observation"
	// PeerObservationConnected is the value of PeerObservationAnnotationKey when the peer is connected.
	PeerObservationConnected = "Connected"
	// PeerObservationDisconnected is the value of PeerObservationAnnotationKey when the peer is disconnected.
	PeerObservationDisconnected = "Disconnected"
	// PeerObservationUnconfirmed is the value of PeerObservationAnnotationKey when the peer state cannot be confirmed.
	PeerObservationUnconfirmed = "Unconfirmed"
	// PeerObservationStaleAnnotationKey is the annotation marking the reported observation as stale, as the current
	// connectivity cannot be confirmed.
	PeerObservationStaleAnnotationKey = "liqo.io/peer-observation-stale"
	// PeerLastObservedReadyAnnotationKey is the annotation reporting the last time the peer was observed as connected.
	// It is preserved while the peer is disconnected, so that the last successful observation remains available
	// (marked as stale) instead of pretending the remote cluster is still serving.
	PeerLastObservedReadyAnnotationKey = "liqo.io/peer-last-observed-ready"
)
