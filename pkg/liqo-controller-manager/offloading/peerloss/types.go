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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Default timing parameters.
//
// These values are provisional: whether both the confirmation and the recovery side should be debounced,
// as well as the exact default durations, still need to be confirmed with the maintainers.
const (
	// DefaultConfirmationWindow is how long a network loss must persist before it is considered confirmed.
	DefaultConfirmationWindow = 1 * time.Minute
	// DefaultRecoveryWindow is how long connectivity must remain stable before the unavailable mark is cleared.
	DefaultRecoveryWindow = 30 * time.Second
	// DefaultRetentionPeriod is the extra grace granted, after the loss has been confirmed, before eviction.
	DefaultRetentionPeriod = 0 * time.Second
	// DefaultResyncInterval is the fallback re-evaluation period used while the peer state cannot be confirmed.
	DefaultResyncInterval = 1 * time.Minute
)

// Policy is the effective disposition applied to a Pod whose peer is lost.
type Policy string

const (
	// PolicyRetain keeps the Pod (and its remote counterpart) untouched, only marking it unavailable.
	PolicyRetain Policy = "Retain"
	// PolicyEvict authorizes eviction (or forced deletion of an already terminating Pod) once the loss is confirmed.
	PolicyEvict Policy = "Evict"
)

// PeerStatus is the connectivity towards the peer cluster as derived from its persistent ForeignCluster condition.
type PeerStatus string

const (
	// PeerConnected indicates the peer networking is established.
	PeerConnected PeerStatus = "Connected"
	// PeerDisconnected indicates the peer networking is not established (pending or error).
	PeerDisconnected PeerStatus = "Disconnected"
	// PeerUnconfirmed indicates the connectivity cannot be confirmed (missing condition, missing transition timestamp,
	// or no reason to trust the current state). The policy never guesses in this case.
	PeerUnconfirmed PeerStatus = "Unconfirmed"
)

// LabelDecision is the desired state of the local `liqo.io/remote-unavailable` label.
type LabelDecision string

const (
	// LabelKeep leaves the current label untouched (e.g. recovery or confirmation window still pending).
	LabelKeep LabelDecision = "Keep"
	// LabelPresent ensures the remote unavailable label is present.
	LabelPresent LabelDecision = "Present"
	// LabelAbsent ensures the remote unavailable label is absent.
	LabelAbsent LabelDecision = "Absent"
)

// Action is the operation the controller is allowed to execute on the Pod.
type Action string

const (
	// ActionNone means no destructive operation is required.
	ActionNone Action = "None"
	// ActionEvict requests the graceful eviction of the Pod (respecting PodDisruptionBudgets).
	ActionEvict Action = "Evict"
	// ActionForceDelete hard-deletes the Pod, used for already terminating objects on a confirmed lost peer.
	ActionForceDelete Action = "ForceDelete"
)

// Reason enumerates the machine-readable reasons attached to a Decision.
const (
	ReasonPeerConnected          = "PeerConnected"
	ReasonPeerRecovered          = "PeerRecovered"
	ReasonRecoveryPending        = "RecoveryPending"
	ReasonPeerUnconfirmed        = "PeerUnconfirmed"
	ReasonLossPending            = "LossPending"
	ReasonRetained               = "Retained"
	ReasonEvictionDisabled       = "EvictionDisabled"
	ReasonEvictionPending        = "EvictionPending"
	ReasonEvictionDue            = "EvictionDue"
	ReasonForceDeleteTerminating = "ForceDeleteTerminating"
)

// PodCategory groups Pods by the kind of their controller, so that the default policy can distinguish
// stateful workloads (which must be preserved) from stateless ones (where recreation may be acceptable).
type PodCategory string

const (
	// CategoryStateless identifies Pods managed by a Deployment or ReplicaSet.
	CategoryStateless PodCategory = "Stateless"
	// CategoryStateful identifies Pods managed by a StatefulSet.
	CategoryStateful PodCategory = "Stateful"
	// CategoryDaemon identifies Pods managed by a DaemonSet.
	CategoryDaemon PodCategory = "Daemon"
	// CategoryJob identifies Pods managed by a Job or CronJob.
	CategoryJob PodCategory = "Job"
	// CategoryBare identifies Pods without a controller owner (bare Pods).
	CategoryBare PodCategory = "Bare"
	// CategoryOther identifies Pods owned by any other controller.
	CategoryOther PodCategory = "Other"
)

// Params configures the decision. Zero values are replaced by the documented defaults.
type Params struct {
	// ConfirmationWindow is the minimum time a loss must persist before it is confirmed.
	ConfirmationWindow time.Duration
	// RecoveryWindow is the minimum time connectivity must persist before clearing the mark.
	RecoveryWindow time.Duration
	// RetentionPeriod is the extra time, after confirmation, before an authorized eviction is requested.
	RetentionPeriod time.Duration
	// ResyncInterval is the fallback re-evaluation period while the peer state is unconfirmed.
	ResyncInterval time.Duration
	// StatelessPolicy is the default policy for stateless workloads. It is left as a parameter, and defaults to
	// Retain, because automatically evicting stateless Pods is not yet confirmed as safe (recreation while the
	// remote copy might still be running can produce dual-active). Change it only once that decision is made.
	StatelessPolicy Policy
	// AllowEviction is the global switch. When false, no destructive action is ever emitted, even for Pods that
	// explicitly request eviction.
	AllowEviction bool
}

// WithDefaults returns a copy of the parameters with defaults applied to unset fields.
func (p Params) WithDefaults() Params {
	if p.ConfirmationWindow <= 0 {
		p.ConfirmationWindow = DefaultConfirmationWindow
	}
	if p.RecoveryWindow <= 0 {
		p.RecoveryWindow = DefaultRecoveryWindow
	}
	if p.ResyncInterval <= 0 {
		p.ResyncInterval = DefaultResyncInterval
	}
	if p.RetentionPeriod < 0 {
		p.RetentionPeriod = DefaultRetentionPeriod
	}
	if p.StatelessPolicy != PolicyEvict {
		p.StatelessPolicy = PolicyRetain
	}
	return p
}

// NodeSignal summarizes the signals carried by the virtual Node conditions, so that the policy can
// distinguish a network loss from resource exhaustion or an operator cordon/drain.
type NodeSignal struct {
	// Ready is the NodeReady condition.
	Ready bool
	// NetworkUnavailable is the NodeNetworkUnavailable condition being True.
	NetworkUnavailable bool
	// ResourcesReady is whether the advertised CPU and memory are greater than zero (no pressure conditions).
	ResourcesReady bool
	// Unschedulable is the Node cordon bit. It is reported for completeness only: a cordon does not by itself
	// imply a network loss.
	Unschedulable bool
}

// PeerState is the connectivity towards the peer cluster.
type PeerState struct {
	// Status is the connectivity state.
	Status PeerStatus
	// Since is the instant at which the current Status was entered (the persistent LastTransitionTime).
	// It is used as the anchor so that controller restarts do not reset the timers. A zero value means the
	// anchor is unknown, which forces Status to Unconfirmed during decision.
	Since time.Time
}

// PodState captures the Pod attributes relevant to the decision.
type PodState struct {
	// Category is the Pod controller category.
	Category PodCategory
	// Terminating is whether the Pod has a non-zero deletion timestamp.
	Terminating bool
	// PolicyOverride is the explicit policy requested through the Pod annotation, or empty if not set.
	PolicyOverride Policy
	// HasRemoteUnavailableLabel is the current state of the remote unavailable label.
	HasRemoteUnavailableLabel bool
}

// Input is the complete, side-effect-free input of the decision.
type Input struct {
	Now    time.Time
	Node   NodeSignal
	Peer   PeerState
	Pod    PodState
	Params Params
}

// Decision is the outcome of the policy evaluation.
type Decision struct {
	// Label is the desired state of the remote unavailable label.
	Label LabelDecision
	// Action is the destructive operation allowed by the policy.
	Action Action
	// Reason is the machine-readable reason for the decision.
	Reason string
	// NextEvaluation is the instant at which the decision must be re-evaluated. A zero value means no deadline
	// is pending and the controller may rely on watches/resync. The controller schedules it through RequeueAfter.
	NextEvaluation time.Time
	// EffectivePolicy is the policy actually applied (after override resolution and the global switch).
	EffectivePolicy Policy
	// Peer contains the peer status the decision was based on.
	Peer PeerStatus
}

// CategoryFromOwnerReferences returns the Pod category from its controller owner reference.
// It returns CategoryBare if no controller owner reference is present.
func CategoryFromOwnerReferences(refs []metav1.OwnerReference) PodCategory {
	var category PodCategory
	found := false
	for i := range refs {
		ref := refs[i]
		if ref.Controller == nil || !*ref.Controller {
			continue
		}
		found = true
		switch ref.Kind {
		case "ReplicaSet", "Deployment":
			category = CategoryStateless
		case "StatefulSet":
			category = CategoryStateful
		case "DaemonSet":
			category = CategoryDaemon
		case "Job", "CronJob":
			category = CategoryJob
		default:
			category = CategoryOther
		}
		// A Pod has a single controller; stop at the first one.
		break
	}
	if !found {
		return CategoryBare
	}
	return category
}

// resolvePolicy returns the policy to apply to the Pod, before the global switch is enforced.
func resolvePolicy(pod PodState, params Params) Policy {
	switch pod.PolicyOverride {
	case PolicyRetain:
		return PolicyRetain
	case PolicyEvict:
		return PolicyEvict
	}

	switch pod.Category {
	case CategoryStateless:
		return params.StatelessPolicy
	default:
		// Stateful, Daemon, Job, Bare and unknown controllers are always retained by default.
		return PolicyRetain
	}
}
