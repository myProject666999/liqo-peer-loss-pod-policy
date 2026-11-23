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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	liqov1beta1 "github.com/liqotech/liqo/apis/core/v1beta1"
	"github.com/liqotech/liqo/pkg/consts"
)

func TestDecide(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	params := Params{
		ConfirmationWindow: 1 * time.Minute,
		RecoveryWindow:     30 * time.Second,
		RetentionPeriod:    0,
		ResyncInterval:     1 * time.Minute,
		AllowEviction:      true,
	}

	connected := func() PeerState {
		return PeerState{Status: PeerConnected, Since: now.Add(-1 * time.Hour)}
	}
	disconnected := func(since time.Duration) PeerState {
		return PeerState{Status: PeerDisconnected, Since: now.Add(since)}
	}
	stateless := func(override Policy, terminating, labelled bool) PodState {
		return PodState{
			Category:                  CategoryStateless,
			PolicyOverride:            override,
			Terminating:               terminating,
			HasRemoteUnavailableLabel: labelled,
		}
	}

	tests := []struct {
		name       string
		in         Input
		wantLabel  LabelDecision
		wantAction Action
		wantReason string
		wantNext   time.Time
	}{
		{
			name:       "unconfirmed peer does not mutate anything",
			in:         Input{Now: now, Peer: PeerState{Status: PeerUnconfirmed}, Pod: stateless(PolicyEvict, false, false), Params: params},
			wantLabel:  LabelKeep,
			wantAction: ActionNone,
			wantReason: ReasonPeerUnconfirmed,
			wantNext:   now.Add(time.Minute),
		},
		{
			name:       "disconnection without transition time is unconfirmed",
			in:         Input{Now: now, Peer: PeerState{Status: PeerDisconnected}, Pod: stateless(PolicyEvict, false, false), Params: params},
			wantLabel:  LabelKeep,
			wantAction: ActionNone,
			wantReason: ReasonPeerUnconfirmed,
			wantNext:   now.Add(time.Minute),
		},
		{
			name:       "loss within the confirmation window only marks and requeues",
			in:         Input{Now: now, Peer: disconnected(-30 * time.Second), Pod: stateless(PolicyEvict, false, false), Params: params},
			wantLabel:  LabelPresent,
			wantAction: ActionNone,
			wantReason: ReasonLossPending,
			wantNext:   now.Add(30 * time.Second),
		},
		{
			name:       "retain policy keeps the pod after the window",
			in:         Input{Now: now, Peer: disconnected(-2 * time.Minute), Pod: stateless("", false, false), Params: params},
			wantLabel:  LabelPresent,
			wantAction: ActionNone,
			wantReason: ReasonRetained,
			wantNext:   now.Add(time.Minute),
		},
		{
			name: "explicit eviction is gated by the global switch",
			in: Input{Now: now, Peer: disconnected(-2 * time.Minute), Pod: stateless(PolicyEvict, false, false),
				Params: func() Params { p := params; p.AllowEviction = false; return p }()},
			wantLabel:  LabelPresent,
			wantAction: ActionNone,
			wantReason: ReasonEvictionDisabled,
			wantNext:   now.Add(time.Minute),
		},
		{
			name:       "explicit eviction is emitted after the window",
			in:         Input{Now: now, Peer: disconnected(-2 * time.Minute), Pod: stateless(PolicyEvict, false, false), Params: params},
			wantLabel:  LabelPresent,
			wantAction: ActionEvict,
			wantReason: ReasonEvictionDue,
		},
		{
			name:       "already terminating pods are force-deleted, not evicted",
			in:         Input{Now: now, Peer: disconnected(-2 * time.Minute), Pod: stateless(PolicyEvict, true, false), Params: params},
			wantLabel:  LabelPresent,
			wantAction: ActionForceDelete,
			wantReason: ReasonForceDeleteTerminating,
		},
		{
			name:       "stateful pods are retained even when the peer is lost",
			in:         Input{Now: now, Peer: disconnected(-2 * time.Minute), Pod: PodState{Category: CategoryStateful}, Params: params},
			wantLabel:  LabelPresent,
			wantAction: ActionNone,
			wantReason: ReasonRetained,
			wantNext:   now.Add(time.Minute),
		},
		{
			name: "bare pods are retained even when the peer is lost",
			in: Input{Now: now, Peer: disconnected(-2 * time.Minute),
				Pod: PodState{Category: CategoryBare}, Params: params},
			wantLabel:  LabelPresent,
			wantAction: ActionNone,
			wantReason: ReasonRetained,
			wantNext:   now.Add(time.Minute),
		},
		{
			name: "recovery within the stability window keeps the mark",
			in: Input{Now: now, Peer: PeerState{Status: PeerConnected, Since: now.Add(-10 * time.Second)},
				Pod: stateless("", false, true), Params: params},
			wantLabel:  LabelKeep,
			wantAction: ActionNone,
			wantReason: ReasonRecoveryPending,
			wantNext:   now.Add(20 * time.Second),
		},
		{
			name: "recovery clears the mark after the stability window",
			in: Input{Now: now, Peer: PeerState{Status: PeerConnected, Since: now.Add(-1 * time.Minute)},
				Pod: stateless("", false, true), Params: params},
			wantLabel:  LabelAbsent,
			wantAction: ActionNone,
			wantReason: ReasonPeerRecovered,
		},
		{
			name:       "connected pods without the mark are untouched",
			in:         Input{Now: now, Peer: connected(), Pod: stateless("", false, false), Params: params},
			wantLabel:  LabelAbsent,
			wantAction: ActionNone,
			wantReason: ReasonPeerConnected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Decide(tt.in)
			if got.Label != tt.wantLabel {
				t.Errorf("Label = %q, want %q", got.Label, tt.wantLabel)
			}
			if got.Action != tt.wantAction {
				t.Errorf("Action = %q, want %q", got.Action, tt.wantAction)
			}
			if got.Reason != tt.wantReason {
				t.Errorf("Reason = %q, want %q", got.Reason, tt.wantReason)
			}
			if !got.NextEvaluation.Equal(tt.wantNext) {
				t.Errorf("NextEvaluation = %v, want %v", got.NextEvaluation, tt.wantNext)
			}
		})
	}
}

func TestDecideRepeatedLossRestartsTheTimer(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	params := Params{ConfirmationWindow: time.Minute, RecoveryWindow: 30 * time.Second, AllowEviction: true}
	pod := PodState{Category: CategoryStateless, PolicyOverride: PolicyEvict}

	// First loss confirmed.
	first := Decide(Input{Now: now, Peer: PeerState{Status: PeerDisconnected, Since: now.Add(-2 * time.Minute)},
		Pod: pod, Params: params})
	if first.Action != ActionEvict {
		t.Fatalf("expected eviction on first confirmed loss, got %q", first.Action)
	}

	// The peer flaps: a new disconnection carries a fresh LastTransitionTime, hence the confirmation window restarts
	// and no destructive action is emitted immediately.
	second := Decide(Input{Now: now, Peer: PeerState{Status: PeerDisconnected, Since: now.Add(-10 * time.Second)},
		Pod: pod, Params: params})
	if second.Action != ActionNone || second.Reason != ReasonLossPending {
		t.Fatalf("expected a pending loss after flapping, got action=%q reason=%q", second.Action, second.Reason)
	}
	if !second.NextEvaluation.Equal(now.Add(50 * time.Second)) {
		t.Fatalf("expected re-evaluation 50s from now, got %v", second.NextEvaluation)
	}
}

func TestDecideZeroResourcesIsNotPeerLoss(t *testing.T) {
	// A NotReady node advertising zero resources must not be treated as a peer network loss.
	node := NodeSignal{Ready: false, ResourcesReady: false, NetworkUnavailable: true}
	if NodeIndicatesPeerLoss(node) {
		t.Fatalf("zero resources must not be interpreted as a peer loss")
	}

	// A cordoned node is not a network loss either.
	cordoned := NodeSignal{Ready: false, ResourcesReady: true, Unschedulable: true, NetworkUnavailable: false}
	if NodeIndicatesPeerLoss(cordoned) {
		t.Fatalf("a cordon must not be interpreted as a peer loss")
	}

	// A genuine network failure is.
	networkDown := NodeSignal{Ready: false, ResourcesReady: true, NetworkUnavailable: true}
	if !NodeIndicatesPeerLoss(networkDown) {
		t.Fatalf("a network-unavailable node with resources must be interpreted as a peer loss")
	}
}

func TestPeerStateFromForeignCluster(t *testing.T) {
	since := metav1.NewTime(time.Date(2026, 11, 24, 11, 0, 0, 0, time.UTC))

	tests := []struct {
		name string
		fc   *liqov1beta1.ForeignCluster
		want PeerStatus
	}{
		{
			name: "nil foreign cluster is unconfirmed",
			fc:   nil,
			want: PeerUnconfirmed,
		},
		{
			name: "networking disabled is connected",
			fc: &liqov1beta1.ForeignCluster{Status: liqov1beta1.ForeignClusterStatus{
				Modules: liqov1beta1.Modules{Networking: liqov1beta1.Module{Enabled: false}},
			}},
			want: PeerConnected,
		},
		{
			name: "missing condition is unconfirmed",
			fc: &liqov1beta1.ForeignCluster{Status: liqov1beta1.ForeignClusterStatus{
				Modules: liqov1beta1.Modules{Networking: liqov1beta1.Module{Enabled: true}},
			}},
			want: PeerUnconfirmed,
		},
		{
			name: "established condition is connected",
			fc:   fcWithCondition(liqov1beta1.ConditionStatusEstablished, since),
			want: PeerConnected,
		},
		{
			name: "error condition is disconnected",
			fc:   fcWithCondition(liqov1beta1.ConditionStatusError, since),
			want: PeerDisconnected,
		},
		{
			name: "pending condition is disconnected",
			fc:   fcWithCondition(liqov1beta1.ConditionStatusPending, since),
			want: PeerDisconnected,
		},
		{
			name: "error condition without transition time is unconfirmed",
			fc:   fcWithCondition(liqov1beta1.ConditionStatusError, metav1.Time{}),
			want: PeerUnconfirmed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PeerStateFromForeignCluster(tt.fc); got.Status != tt.want {
				t.Errorf("PeerStatus = %q, want %q", got.Status, tt.want)
			}
		})
	}
}

func fcWithCondition(status liqov1beta1.ConditionStatusType, since metav1.Time) *liqov1beta1.ForeignCluster {
	return &liqov1beta1.ForeignCluster{Status: liqov1beta1.ForeignClusterStatus{
		Modules: liqov1beta1.Modules{Networking: liqov1beta1.Module{
			Enabled: true,
			Conditions: []liqov1beta1.Condition{{
				Type:               liqov1beta1.NetworkConnectionStatusCondition,
				Status:             status,
				LastTransitionTime: since,
			}},
		}},
	}}
}

func TestPodStateFromPod(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{consts.PeerLossPolicyAnnotationKey: consts.PeerLossPolicyEvict},
			Labels:      map[string]string{consts.RemoteUnavailableKey: consts.RemoteUnavailableValue},
			OwnerReferences: []metav1.OwnerReference{{
				Kind: "StatefulSet", Controller: ptr.To(true),
			}},
		},
	}

	state := PodStateFromPod(pod)
	if state.Category != CategoryStateful {
		t.Errorf("Category = %q, want %q", state.Category, CategoryStateful)
	}
	if state.PolicyOverride != PolicyEvict {
		t.Errorf("PolicyOverride = %q, want %q", state.PolicyOverride, PolicyEvict)
	}
	if !state.HasRemoteUnavailableLabel {
		t.Error("expected the remote unavailable label to be detected")
	}
	if state.Terminating {
		t.Error("expected the pod not to be terminating")
	}
}

func TestCategoryFromOwnerReferences(t *testing.T) {
	tests := []struct {
		name string
		refs []metav1.OwnerReference
		want PodCategory
	}{
		{name: "no owner", refs: nil, want: CategoryBare},
		{name: "replicaset", refs: []metav1.OwnerReference{{Kind: "ReplicaSet", Controller: ptr.To(true)}}, want: CategoryStateless},
		{name: "statefulset", refs: []metav1.OwnerReference{{Kind: "StatefulSet", Controller: ptr.To(true)}}, want: CategoryStateful},
		{name: "daemonset", refs: []metav1.OwnerReference{{Kind: "DaemonSet", Controller: ptr.To(true)}}, want: CategoryDaemon},
		{name: "job", refs: []metav1.OwnerReference{{Kind: "Job", Controller: ptr.To(true)}}, want: CategoryJob},
		{name: "other", refs: []metav1.OwnerReference{{Kind: "Custom", Controller: ptr.To(true)}}, want: CategoryOther},
		{name: "non-controller owner is ignored", refs: []metav1.OwnerReference{{Kind: "ReplicaSet", Controller: ptr.To(false)}}, want: CategoryBare},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CategoryFromOwnerReferences(tt.refs); got != tt.want {
				t.Errorf("Category = %q, want %q", got, tt.want)
			}
		})
	}
}
