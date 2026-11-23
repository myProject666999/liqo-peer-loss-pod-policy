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

package podstatusctrl

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	liqov1beta1 "github.com/liqotech/liqo/apis/core/v1beta1"
	"github.com/liqotech/liqo/pkg/consts"
	"github.com/liqotech/liqo/pkg/liqo-controller-manager/offloading/peerloss"
	"github.com/liqotech/liqo/pkg/utils"
	fcutils "github.com/liqotech/liqo/pkg/utils/foreigncluster"
	"github.com/liqotech/liqo/pkg/utils/indexer"
)

// HasRemoteUnavailableLabel return true if the pod has the remote unavailable label.
func HasRemoteUnavailableLabel(pod *corev1.Pod) bool {
	value, ok := pod.Labels[consts.RemoteUnavailableKey]
	return ok && value == consts.RemoteUnavailableValue
}

// PodStatusReconciler reconciles Liqo nodes.
//
// For every local offloaded Pod scheduled on a Liqo virtual node, it derives the connectivity towards the peer cluster
// from the persistent ForeignCluster status and applies the peer-loss Pod policy (see the peerloss package): it marks
// the Pod as remotely unavailable, and only when explicitly authorized evicts it (or force-deletes it if already
// terminating). The reconciler itself only executes the actions returned by the pure decision function, and schedules
// the next evaluation through RequeueAfter so that pending deadlines are honored even without external events.
type PodStatusReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Params configures the peer-loss policy. Zero values fall back to the documented defaults.
	Params peerloss.Params

	// Now returns the current time. It is overridable in tests; nil means time.Now.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch;
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups=core.liqo.io,resources=foreignclusters,verbs=get;list;watch;

func (r *PodStatusReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Reconcile reconciles on Liqo nodes and manages the labels of the pods scheduled on them.
func (r *PodStatusReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Fetch the Node instance
	var node corev1.Node
	if err := r.Client.Get(ctx, client.ObjectKey{Name: req.Name}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			klog.V(4).Infof("node %s not found", req.Name)
			return ctrl.Result{}, nil
		}
		klog.Errorf("an error occurred while getting node %s: %v", req.Name, err)
		return ctrl.Result{}, err
	}

	// Skip reconciliation if it is not a Liqo virtual node
	if !utils.IsVirtualNode(&node) {
		klog.V(6).Infof("Skipping reconciliation: node %q is not a Liqo virtual node", req.Name)
		return ctrl.Result{}, nil
	}

	klog.V(4).Infof("reconciliation on liqo virtual node: %q", req.Name)

	// List all pods that are local and offloaded (running in another cluster), and scheduled on the reconciled liqo node
	var pods corev1.PodList
	localOffloadedPodSelector := client.MatchingLabels{consts.LocalPodLabelKey: consts.LocalPodLabelValue}
	nodeSelector := client.MatchingFields{indexer.FieldNodeNameFromPod: node.Name}
	if err := r.List(ctx, &pods, localOffloadedPodSelector, nodeSelector); err != nil {
		klog.Errorf("unable to list pods: %v", err)
		return ctrl.Result{}, err
	}

	// Derive the peer connectivity from the persistent ForeignCluster status. Never infer a cross-cluster loss from an
	// arbitrary Node NotReady: a resource exhaustion or an operator cordon must not trigger the peer-loss policy.
	peerState := r.resolvePeerState(ctx, &node)
	nodeSignal := peerloss.NodeSignalFromNode(&node)
	params := r.Params.WithDefaults()

	// Re-check the peer connectivity right before any destructive action, to avoid acting on stale input (the peer may
	// have recovered while the Pods were being inspected).
	var destructivePeerState *peerloss.PeerState

	var earliest *time.Time
	var needResync bool
	for i := range pods.Items {
		pod := &pods.Items[i]

		decision := peerloss.Decide(peerloss.Input{
			Now:    r.now(),
			Node:   nodeSignal,
			Peer:   peerState,
			Pod:    peerloss.PodStateFromPod(pod),
			Params: params,
		})
		klog.V(5).Infof("peer-loss policy for pod %q: peer=%s label=%s action=%s policy=%s reason=%s",
			klog.KObj(pod), decision.Peer, decision.Label, decision.Action, decision.EffectivePolicy, decision.Reason)

		if err := r.reconcilePodLabel(ctx, pod, decision); err != nil {
			return ctrl.Result{}, err
		}

		if decision.Action == peerloss.ActionNone {
			earliest = earliestOr(earliest, decision.NextEvaluation)
			needResync = needResync || decision.Label == peerloss.LabelPresent
			continue
		}

		// Destructive action: re-fetch the peer state once, and re-validate the Pod right before acting.
		if destructivePeerState == nil {
			state := r.resolvePeerState(ctx, &node)
			destructivePeerState = &state
		}
		if destructivePeerState.Status != peerloss.PeerDisconnected {
			klog.V(4).Infof("skipping %s of pod %q: peer is no longer disconnected (status %q)",
				decision.Action, klog.KObj(pod), destructivePeerState.Status)
			earliest = earliestOr(earliest, decision.NextEvaluation)
			continue
		}

		if err := r.executeAction(ctx, pod, decision); err != nil {
			return ctrl.Result{}, err
		}
		earliest = earliestOr(earliest, decision.NextEvaluation)
	}

	if earliest != nil {
		// Honor the pending deadline even in the absence of external events. A non-positive delay (the deadline already
		// elapsed) must still trigger a reconciliation, hence it is clamped to the smallest positive duration.
		if delay := earliest.Sub(r.now()); delay > 0 {
			return ctrl.Result{RequeueAfter: delay}, nil
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if needResync || peerState.Status != peerloss.PeerConnected {
		// The peer connectivity cannot be observed directly by this controller (it only watches Nodes), so keep
		// re-evaluating while a Pod is marked unavailable or the peer is not confirmed as connected. This ensures the
		// mark is cleared (or a loss is noticed) even when the virtual node conditions do not change.
		return ctrl.Result{RequeueAfter: params.ResyncInterval}, nil
	}

	return ctrl.Result{}, nil
}

// resolvePeerState derives the peer connectivity for the given virtual node.
func (r *PodStatusReconciler) resolvePeerState(ctx context.Context, node *corev1.Node) peerloss.PeerState {
	clusterID, ok := utils.GetNodeClusterID(node)
	if !ok || clusterID == "" {
		// Without a peer identity we cannot confirm the connectivity, hence do not guess.
		return peerloss.PeerState{Status: peerloss.PeerUnconfirmed}
	}

	fc, err := fcutils.GetForeignClusterByID(ctx, r.Client, liqov1beta1ClusterID(clusterID))
	switch {
	case apierrors.IsNotFound(err):
		// The peer is gone: its state cannot be confirmed.
		return peerloss.PeerState{Status: peerloss.PeerUnconfirmed}
	case err != nil:
		klog.Errorf("unable to get the foreign cluster %q: %v", clusterID, err)
		return peerloss.PeerState{Status: peerloss.PeerUnconfirmed}
	default:
		return peerloss.PeerStateFromForeignCluster(fc)
	}
}

// reconcilePodLabel enforces the desired state of the remote unavailable label on the given Pod.
func (r *PodStatusReconciler) reconcilePodLabel(ctx context.Context, pod *corev1.Pod, decision peerloss.Decision) error {
	switch decision.Label {
	case peerloss.LabelPresent:
		if HasRemoteUnavailableLabel(pod) {
			return nil
		}
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels[consts.RemoteUnavailableKey] = consts.RemoteUnavailableValue
		if err := r.Update(ctx, pod); err != nil {
			klog.Errorf("failed to update pod %q: %v", klog.KObj(pod), err)
			return err
		}
		klog.Infof("Added label %q to pod %q (reason: %s)", consts.RemoteUnavailableKey, klog.KObj(pod), decision.Reason)

	case peerloss.LabelAbsent:
		if !HasRemoteUnavailableLabel(pod) {
			return nil
		}
		delete(pod.Labels, consts.RemoteUnavailableKey)
		if err := r.Update(ctx, pod); err != nil {
			klog.Errorf("failed to update pod %q: %v", klog.KObj(pod), err)
			return err
		}
		klog.Infof("Removed label %q from pod %q (reason: %s)", consts.RemoteUnavailableKey, klog.KObj(pod), decision.Reason)

	case peerloss.LabelKeep:
		// No change required.
	}
	return nil
}

// executeAction performs the destructive action decided for the given Pod.
//
// Before acting it re-fetches the Pod and confirms that the object has not been replaced (same UID) and, for the
// forced deletion, that it is still terminating. This prevents deleting a new replica using a timer computed for an
// old one, as well as acting on a Pod that is no longer the one the decision was based on.
func (r *PodStatusReconciler) executeAction(ctx context.Context, pod *corev1.Pod, decision peerloss.Decision) error {
	// Re-fetch the Pod to validate the UID and the termination state.
	var current corev1.Pod
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(pod), &current); err != nil {
		if apierrors.IsNotFound(err) {
			// The Pod vanished (e.g. it was removed): nothing left to do.
			return nil
		}
		klog.Errorf("unable to re-fetch pod %q before %s: %v", klog.KObj(pod), decision.Action, err)
		return err
	}
	if current.UID != pod.UID {
		klog.Infof("skipping %s of pod %q: the object has been replaced (UID changed)", decision.Action, klog.KObj(pod))
		return nil
	}

	switch decision.Action {
	case peerloss.ActionEvict:
		if !current.DeletionTimestamp.IsZero() {
			// The Pod became terminating after the decision: let the next reconciliation force-delete it if needed.
			klog.V(4).Infof("skipping eviction of pod %q: it is now terminating", klog.KObj(&current))
			return nil
		}
		return r.evictPod(ctx, &current)

	case peerloss.ActionForceDelete:
		if current.DeletionTimestamp.IsZero() {
			// The Pod is no longer terminating: it converged, do not force-delete it.
			klog.V(4).Infof("skipping forced deletion of pod %q: it is no longer terminating", klog.KObj(&current))
			return nil
		}
		return r.forceDeletePod(ctx, &current)

	case peerloss.ActionNone:
		return nil
	}
	return nil
}

// evictPod requests the eviction of the Pod through the Eviction API, so that PodDisruptionBudgets are respected.
func (r *PodStatusReconciler) evictPod(ctx context.Context, pod *corev1.Pod) error {
	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
		DeleteOptions: &metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &pod.UID},
		},
	}
	if err := r.Client.SubResource("eviction").Create(ctx, pod, eviction); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("unable to evict pod %q: %v", klog.KObj(pod), err)
		return err
	}
	klog.Infof("Evicted pod %q from unreachable peer", klog.KObj(pod))
	return nil
}

// forceDeletePod hard-deletes an already terminating Pod that cannot converge while the peer is unreachable.
func (r *PodStatusReconciler) forceDeletePod(ctx context.Context, pod *corev1.Pod) error {
	gracePeriod := int64(0)
	opts := &client.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
		Preconditions:      &metav1.Preconditions{UID: &pod.UID},
	}
	if err := r.Client.Delete(ctx, pod, opts); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("unable to force-delete pod %q: %v", klog.KObj(pod), err)
		return err
	}
	klog.Infof("Force-deleted terminating pod %q from unreachable peer", klog.KObj(pod))
	return nil
}

// earliestOr returns the earliest non-zero instant among the given one and the candidate.
func earliestOr(earliest *time.Time, candidate time.Time) *time.Time {
	if candidate.IsZero() {
		return earliest
	}
	if earliest == nil || candidate.Before(*earliest) {
		value := candidate
		return &value
	}
	return earliest
}

// liqov1beta1ClusterID converts a raw cluster ID string to the API type, keeping the dependency localized.
func liqov1beta1ClusterID(id string) liqov1beta1.ClusterID {
	return liqov1beta1.ClusterID(id)
}

// SetupWithManager monitors updates on nodes.
func (r *PodStatusReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).Named(consts.CtrlPodStatus).
		For(&corev1.Node{}).
		Complete(r)
}
