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

package peerloss

import (
	corev1 "k8s.io/api/core/v1"

	liqov1beta1 "github.com/liqotech/liqo/apis/core/v1beta1"
	"github.com/liqotech/liqo/pkg/consts"
	"github.com/liqotech/liqo/pkg/utils"
	fcutils "github.com/liqotech/liqo/pkg/utils/foreigncluster"
)

// NodeSignalFromNode extracts the relevant node signals from a virtual Node.
func NodeSignalFromNode(node *corev1.Node) NodeSignal {
	if node == nil {
		return NodeSignal{}
	}
	return NodeSignal{
		Ready:              utils.IsNodeReady(node),
		NetworkUnavailable: hasConditionStatus(node, corev1.NodeNetworkUnavailable, corev1.ConditionTrue),
		ResourcesReady:     !hasPressure(node),
		Unschedulable:      node.Spec.Unschedulable,
	}
}

// hasConditionStatus returns true if the node has the given condition with the given status.
func hasConditionStatus(node *corev1.Node, conditionType corev1.NodeConditionType, status corev1.ConditionStatus) bool {
	for i := range node.Status.Conditions {
		condition := &node.Status.Conditions[i]
		if condition.Type == conditionType {
			return condition.Status == status
		}
	}
	return false
}

// hasPressure returns true if any of the resource pressure conditions is set, which indicates that the remote
// cluster is not advertising sufficient resources (i.e. the NotReady status is not a network loss).
func hasPressure(node *corev1.Node) bool {
	for _, conditionType := range []corev1.NodeConditionType{
		corev1.NodeMemoryPressure, corev1.NodeDiskPressure, corev1.NodePIDPressure,
	} {
		if hasConditionStatus(node, conditionType, corev1.ConditionTrue) {
			return true
		}
	}
	return false
}

// PeerStateFromForeignCluster derives the peer connectivity from the persistent ForeignCluster status.
//
// The connectivity is read from the NetworkConnectionStatus module condition, and the transition instant is taken
// from its LastTransitionTime, so that the debounce timers survive controller restarts. When the networking module is
// disabled there is no network layer to gate on, hence the peer is considered connected. A missing condition (or a
// missing transition timestamp) yields Unconfirmed, never a guess.
func PeerStateFromForeignCluster(foreignCluster *liqov1beta1.ForeignCluster) PeerState {
	if foreignCluster == nil {
		return PeerState{Status: PeerUnconfirmed}
	}

	if !fcutils.IsNetworkingModuleEnabled(foreignCluster) {
		// Networking disabled: there is no cross-cluster network to monitor, do not gate the peer on it.
		return PeerState{Status: PeerConnected}
	}

	condition := fcutils.GetCondition(foreignCluster.Status.Modules.Networking.Conditions,
		liqov1beta1.NetworkConnectionStatusCondition)
	if condition == nil {
		return PeerState{Status: PeerUnconfirmed}
	}

	since := condition.LastTransitionTime.Time
	switch condition.Status {
	case liqov1beta1.ConditionStatusEstablished:
		return PeerState{Status: PeerConnected, Since: since}
	case liqov1beta1.ConditionStatusPending, liqov1beta1.ConditionStatusError:
		if since.IsZero() {
			// A disconnection without a transition timestamp cannot be debounced.
			return PeerState{Status: PeerUnconfirmed}
		}
		return PeerState{Status: PeerDisconnected, Since: since}
	default:
		// None or unexpected status: the connectivity is not confirmed.
		return PeerState{Status: PeerUnconfirmed}
	}
}

// PodStateFromPod extracts the Pod attributes relevant to the decision.
func PodStateFromPod(pod *corev1.Pod) PodState {
	if pod == nil {
		return PodState{}
	}

	var override Policy
	switch pod.Annotations[consts.PeerLossPolicyAnnotationKey] {
	case consts.PeerLossPolicyEvict:
		override = PolicyEvict
	case consts.PeerLossPolicyRetain:
		override = PolicyRetain
	}

	value, hasLabel := pod.Labels[consts.RemoteUnavailableKey]
	return PodState{
		Category:                  CategoryFromOwnerReferences(pod.OwnerReferences),
		Terminating:               !pod.DeletionTimestamp.IsZero(),
		PolicyOverride:            override,
		HasRemoteUnavailableLabel: hasLabel && value == consts.RemoteUnavailableValue,
	}
}
