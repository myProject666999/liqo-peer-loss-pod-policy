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

import "time"

// Decide evaluates the peer-loss policy for a local offloaded Pod.
//
// It is a pure function: it performs no I/O, reads no clock and mutates nothing. The controller is expected to
// execute Decision.Action and to re-enqueue the Pod for Decision.NextEvaluation (when non-zero), so that pending
// deadlines are honored even in the absence of external events.
//
// Summary of the behavior:
//   - The connectivity is taken from the peer's persistent condition (Peer.Status), never inferred from an
//     arbitrary Node NotReady. A cordon (Unschedulable) alone never triggers, nor suppresses, a loss.
//   - If the peer state cannot be confirmed, nothing is changed and no destructive action is emitted.
//   - While disconnected, the Pod is marked as remotely unavailable immediately (safe and reversible). Destructive
//     actions are deferred until the loss has been confirmed for the whole confirmation window.
//   - A Pod is only evicted (or force-deleted if already terminating) when its effective policy is Evict, which
//     requires the explicit annotation and the global switch. Otherwise it is retained.
//   - When the peer recovers, the unavailable mark is cleared only after the connectivity has been stable for the
//     recovery window, so that a flapping peer does not cause the mark to oscillate.
func Decide(in Input) Decision {
	params := in.Params.WithDefaults()
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}

	requested := resolvePolicy(in.Pod, params)
	effective := requested
	if effective == PolicyEvict && !params.AllowEviction {
		effective = PolicyRetain
	}

	// Normalize the peer state. A disconnection without a trustworthy transition timestamp cannot be debounced,
	// hence it must be reported as unconfirmed instead of being guessed.
	peer := in.Peer
	if peer.Since.IsZero() && peer.Status == PeerDisconnected {
		peer.Status = PeerUnconfirmed
	}

	switch peer.Status {
	case PeerUnconfirmed:
		// Do not guess: keep the current label and emit no destructive action.
		return Decision{
			Label:           LabelKeep,
			Action:          ActionNone,
			Reason:          ReasonPeerUnconfirmed,
			NextEvaluation:  now.Add(params.ResyncInterval),
			EffectivePolicy: effective,
			Peer:            peer.Status,
		}

	case PeerConnected:
		return decideConnected(in, peer, effective, now, params)

	case PeerDisconnected:
		return decideDisconnected(in, peer, requested, effective, now, params)
	default:
		// Unknown value: be conservative.
		return Decision{
			Label:           LabelKeep,
			Action:          ActionNone,
			Reason:          ReasonPeerUnconfirmed,
			NextEvaluation:  now.Add(params.ResyncInterval),
			EffectivePolicy: effective,
			Peer:            peer.Status,
		}
	}
}

// decideConnected handles the case in which the peer is (or has just become) reachable again.
func decideConnected(in Input, peer PeerState, effective Policy, now time.Time, params Params) Decision {
	decision := Decision{EffectivePolicy: effective, Peer: peer.Status, Action: ActionNone}

	if !in.Pod.HasRemoteUnavailableLabel {
		// Nothing to clean up.
		decision.Label = LabelAbsent
		decision.Reason = ReasonPeerConnected
		return decision
	}

	// The label is present: debounce the recovery to avoid flapping.
	if peer.Since.IsZero() {
		// No trustworthy anchor to debounce against, but clearing the mark is the safe direction (the peer is
		// reachable), so clear it immediately instead of guessing a deadline.
		decision.Label = LabelAbsent
		decision.Reason = ReasonPeerRecovered
		return decision
	}

	deadline := peer.Since.Add(params.RecoveryWindow)
	if now.Before(deadline) {
		decision.Label = LabelKeep
		decision.Reason = ReasonRecoveryPending
		decision.NextEvaluation = deadline
		return decision
	}

	decision.Label = LabelAbsent
	decision.Reason = ReasonPeerRecovered
	return decision
}

// decideDisconnected handles the case in which the peer is currently unreachable.
// requested is the policy resolved from the Pod (before the global switch), effective is the policy actually applied.
func decideDisconnected(in Input, peer PeerState, requested, effective Policy, now time.Time, params Params) Decision {
	decision := Decision{EffectivePolicy: effective, Peer: peer.Status}

	// From this point on the Pod is known to be offloaded to an unreachable peer: mark it as remotely unavailable.
	// This is non-destructive and reversible, and lets the local control plane stop routing traffic to it.
	decision.Label = LabelPresent

	confirmDeadline := peer.Since.Add(params.ConfirmationWindow)
	if now.Before(confirmDeadline) {
		// Debounce: do not perform destructive actions before the loss is confirmed.
		decision.Action = ActionNone
		decision.Reason = ReasonLossPending
		decision.NextEvaluation = confirmDeadline
		return decision
	}

	// Eviction is only allowed when explicitly authorized; the global switch gates it as well.
	if effective != PolicyEvict {
		decision.Action = ActionNone
		if requested == PolicyEvict {
			// The Pod explicitly requested eviction, but the global switch disabled it.
			decision.Reason = ReasonEvictionDisabled
		} else {
			decision.Reason = ReasonRetained
		}
		// Nothing has to happen before the peer recovers, but the connectivity cannot be confirmed from the node watch
		// alone (e.g. when the network check is disabled), so keep re-evaluating periodically to clear the mark once
		// the peer is back.
		decision.NextEvaluation = now.Add(params.ResyncInterval)
		return decision
	}

	if in.Pod.Terminating {
		// The Pod is already being deleted and will not converge while the peer is unreachable: force-delete it so
		// that its controller can recreate a fresh replica.
		decision.Action = ActionForceDelete
		decision.Reason = ReasonForceDeleteTerminating
		return decision
	}

	evictionDeadline := confirmDeadline.Add(params.RetentionPeriod)
	if now.Before(evictionDeadline) {
		decision.Action = ActionNone
		decision.Reason = ReasonEvictionPending
		decision.NextEvaluation = evictionDeadline
		return decision
	}

	decision.Action = ActionEvict
	decision.Reason = ReasonEvictionDue
	return decision
}

// NodeIndicatesPeerLoss reports whether the virtual Node NotReady status is attributable to a network loss,
// rather than to resource exhaustion or an operator cordon/drain.
//
// This is only used to corroborate the peer connectivity for logging and for the node observation annotation: the
// loss decision itself is driven by the peer's persistent condition. The cordon (Unschedulable) bit is deliberately
// ignored, as a cordon does not by itself make the Node NotReady and hence must not be read as a network partition.
func NodeIndicatesPeerLoss(node NodeSignal) bool {
	if node.Ready {
		return false
	}
	// Resource exhaustion is not a network loss.
	if !node.ResourcesReady {
		return false
	}
	// Only a network-unavailable condition (with resources available) identifies a cross-cluster disconnection.
	return node.NetworkUnavailable
}
