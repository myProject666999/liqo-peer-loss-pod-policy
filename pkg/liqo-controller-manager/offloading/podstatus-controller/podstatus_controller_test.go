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
	"bytes"
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	liqov1beta1 "github.com/liqotech/liqo/apis/core/v1beta1"
	"github.com/liqotech/liqo/pkg/consts"
	"github.com/liqotech/liqo/pkg/liqo-controller-manager/offloading/peerloss"
	"github.com/liqotech/liqo/pkg/utils/indexer"
)

var _ = Describe("PodStatusController", func() {
	const (
		ns            string = "default"
		nodeName      string = "node-test"
		liqoNodeName  string = "liqo-node-test"
		liqoNodeName2 string = "liqo-node-test-2"
		podName       string = "test-pod"
		localPodName  string = "test-local-pod"
		localPodName2 string = "test-local-pod-2"
		clusterID     string = "remote-cluster"
	)

	var (
		ctx        context.Context
		buffer     *bytes.Buffer
		fakeClient client.WithWatch
		localPod   *corev1.Pod
		localPod2  *corev1.Pod
		liqoNode2  *corev1.Node
		now        time.Time

		reqLiqoNode = ctrl.Request{NamespacedName: types.NamespacedName{Name: liqoNodeName}}

		reqLocalPod = ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      localPodName,
				Namespace: ns,
			},
		}

		reqLocalPod2 = ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      localPodName2,
				Namespace: ns,
			},
		}

		newLiqoNode = func(name string, networkUnavailable bool) *corev1.Node {
			ready := corev1.ConditionTrue
			if networkUnavailable {
				ready = corev1.ConditionFalse
			}
			return &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: name,
					Labels: map[string]string{
						consts.TypeLabel:       consts.TypeNode,
						consts.RemoteClusterID: clusterID,
					},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: ready},
						{Type: corev1.NodeNetworkUnavailable, Status: boolToCondition(networkUnavailable)},
					},
					Allocatable: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
				},
			}
		}

		// newForeignCluster builds a ForeignCluster carrying the persistent networking condition.
		newForeignCluster = func(status liqov1beta1.ConditionStatusType, since time.Time) *liqov1beta1.ForeignCluster {
			condition := liqov1beta1.Condition{
				Type:   liqov1beta1.NetworkConnectionStatusCondition,
				Status: status,
			}
			if !since.IsZero() {
				condition.LastTransitionTime = metav1.NewTime(since)
			}
			return &liqov1beta1.ForeignCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:   clusterID,
					Labels: map[string]string{consts.RemoteClusterID: clusterID},
				},
				Spec: liqov1beta1.ForeignClusterSpec{ClusterID: liqov1beta1.ClusterID(clusterID)},
				Status: liqov1beta1.ForeignClusterStatus{
					ObservedGeneration: 1,
					Modules: liqov1beta1.Modules{Networking: liqov1beta1.Module{
						Enabled:    true,
						Conditions: []liqov1beta1.Condition{condition},
					}},
				},
			}
		}

		newPod = func() *corev1.Pod {
			return &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: ns},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "nginx", Image: "nginx"}},
					NodeName:   nodeName,
				},
			}
		}

		newLocalPod = func() *corev1.Pod {
			pod := newPod()
			pod.Name = localPodName
			pod.Labels = labels.Merge(pod.Labels, labels.Set{consts.LocalPodLabelKey: consts.LocalPodLabelValue})
			pod.Spec.NodeName = liqoNodeName
			pod.UID = types.UID("uid-1")
			return pod
		}
	)

	BeforeEach(func() {
		ctx = context.Background()
		buffer = &bytes.Buffer{}
		klog.SetOutput(buffer)
		now = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

		localPod = newLocalPod()

		localPod2 = newLocalPod()
		localPod2.Name = localPodName2
		localPod2.Spec.NodeName = liqoNodeName2
		localPod2.UID = types.UID("uid-2")

		liqoNode2 = newLiqoNode(liqoNodeName2, false)
	})

	// reconcile runs the reconciler with the given current time and returns the result.
	reconcile := func(allowEviction bool) ctrl.Result {
		r := &PodStatusReconciler{
			Client: fakeClient,
			Scheme: scheme.Scheme,
			Params: peerloss.Params{
				ConfirmationWindow: time.Minute,
				RecoveryWindow:     30 * time.Second,
				AllowEviction:      allowEviction,
			},
			Now: func() time.Time { return now },
		}
		result, rerr := r.Reconcile(ctx, reqLiqoNode)
		Expect(rerr).NotTo(HaveOccurred())
		klog.Flush()
		return result
	}

	buildClient := func(objs ...client.Object) {
		fakeClient = fake.NewClientBuilder().WithScheme(scheme.Scheme).
			WithIndex(&corev1.Pod{}, indexer.FieldNodeNameFromPod, indexer.ExtractNodeName).
			WithObjects(objs...).Build()
	}

	When("the peer networking is disconnected (confirmed)", func() {
		When("the pod has no remote unavailable label", func() {
			BeforeEach(func() {
				buildClient(newLiqoNode(liqoNodeName, true), liqoNode2,
					newForeignCluster(liqov1beta1.ConditionStatusError, now.Add(-5*time.Minute)), localPod, localPod2)
			})

			It("should add the label only to pods on the reconciled node", func() {
				reconcile(false)
				var after corev1.Pod
				Expect(fakeClient.Get(ctx, reqLocalPod.NamespacedName, &after)).To(Succeed())
				Expect(after.Labels).To(HaveKeyWithValue(consts.RemoteUnavailableKey, consts.RemoteUnavailableValue))

				var after2 corev1.Pod
				Expect(fakeClient.Get(ctx, reqLocalPod2.NamespacedName, &after2)).To(Succeed())
				Expect(after2.Labels).ToNot(HaveKey(consts.RemoteUnavailableKey))
			})

			It("should retain the pod by default, without eviction", func() {
				reconcile(false)
				var after corev1.Pod
				Expect(fakeClient.Get(ctx, reqLocalPod.NamespacedName, &after)).To(Succeed())
				Expect(after.DeletionTimestamp.IsZero()).To(BeTrue())
			})
		})

		When("the loss is still within the confirmation window", func() {
			BeforeEach(func() {
				buildClient(newLiqoNode(liqoNodeName, true), liqoNode2,
					newForeignCluster(liqov1beta1.ConditionStatusError, now.Add(-10*time.Second)), localPod, localPod2)
			})

			It("should mark the pod but requeue for the confirmation deadline", func() {
				result := reconcile(true)
				var after corev1.Pod
				Expect(fakeClient.Get(ctx, reqLocalPod.NamespacedName, &after)).To(Succeed())
				Expect(after.Labels).To(HaveKeyWithValue(consts.RemoteUnavailableKey, consts.RemoteUnavailableValue))
				Expect(after.DeletionTimestamp.IsZero()).To(BeTrue())
				Expect(result.RequeueAfter).To(BeNumerically(">", 0))
				Expect(result.RequeueAfter).To(BeNumerically("<=", time.Minute))
			})
		})

		When("the pod explicitly requests eviction and eviction is allowed", func() {
			BeforeEach(func() {
				localPod.Annotations = map[string]string{consts.PeerLossPolicyAnnotationKey: consts.PeerLossPolicyEvict}
				buildClient(newLiqoNode(liqoNodeName, true), liqoNode2,
					newForeignCluster(liqov1beta1.ConditionStatusError, now.Add(-5*time.Minute)), localPod, localPod2)
			})

			It("should evict the pod through the eviction subresource", func() {
				reconcile(true)
				var after corev1.Pod
				// The fake client deletes the pod on eviction, hence it must be gone.
				err := fakeClient.Get(ctx, reqLocalPod.NamespacedName, &after)
				Expect(err).To(HaveOccurred())
				Expect(buffer.String()).To(ContainSubstring("Evicted pod"))
			})
		})

		When("the pod explicitly requests eviction but eviction is disabled", func() {
			BeforeEach(func() {
				localPod.Annotations = map[string]string{consts.PeerLossPolicyAnnotationKey: consts.PeerLossPolicyEvict}
				buildClient(newLiqoNode(liqoNodeName, true), liqoNode2,
					newForeignCluster(liqov1beta1.ConditionStatusError, now.Add(-5*time.Minute)), localPod, localPod2)
			})

			It("should only mark the pod and keep it", func() {
				reconcile(false)
				var after corev1.Pod
				Expect(fakeClient.Get(ctx, reqLocalPod.NamespacedName, &after)).To(Succeed())
				Expect(after.Labels).To(HaveKeyWithValue(consts.RemoteUnavailableKey, consts.RemoteUnavailableValue))
				Expect(after.DeletionTimestamp.IsZero()).To(BeTrue())
			})
		})

		When("the pod is already terminating and eviction is authorized", func() {
			BeforeEach(func() {
				localPod.Annotations = map[string]string{consts.PeerLossPolicyAnnotationKey: consts.PeerLossPolicyEvict}
				localPod.DeletionTimestamp = ptr.To(metav1.NewTime(now.Add(-30 * time.Second)))
				localPod.Finalizers = []string{"test"}
				buildClient(newLiqoNode(liqoNodeName, true), liqoNode2,
					newForeignCluster(liqov1beta1.ConditionStatusError, now.Add(-5*time.Minute)), localPod, localPod2)
			})

			It("should force-delete the terminating pod so its controller can recreate it", func() {
				reconcile(true)
				Expect(buffer.String()).To(ContainSubstring("Force-deleted terminating pod"))
			})
		})

		When("the pod is a stateful workload", func() {
			BeforeEach(func() {
				localPod.OwnerReferences = []metav1.OwnerReference{{Kind: "StatefulSet", Controller: ptr.To(true)}}
				buildClient(newLiqoNode(liqoNodeName, true), liqoNode2,
					newForeignCluster(liqov1beta1.ConditionStatusError, now.Add(-5*time.Minute)), localPod, localPod2)
			})

			It("should be retained by default, even with eviction allowed", func() {
				reconcile(true)
				var after corev1.Pod
				Expect(fakeClient.Get(ctx, reqLocalPod.NamespacedName, &after)).To(Succeed())
				Expect(after.Labels).To(HaveKeyWithValue(consts.RemoteUnavailableKey, consts.RemoteUnavailableValue))
				Expect(after.DeletionTimestamp.IsZero()).To(BeTrue())
			})
		})
	})

	When("the peer networking is established", func() {
		When("the pod already has the remote unavailable label", func() {
			BeforeEach(func() {
				localPod.Labels = labels.Merge(localPod.Labels, labels.Set{consts.RemoteUnavailableKey: consts.RemoteUnavailableValue})
				buildClient(newLiqoNode(liqoNodeName, false), liqoNode2,
					newForeignCluster(liqov1beta1.ConditionStatusEstablished, now.Add(-5*time.Minute)), localPod, localPod2)
			})

			It("should remove the label after the recovery stability window", func() {
				reconcile(false)
				var after corev1.Pod
				Expect(fakeClient.Get(ctx, reqLocalPod.NamespacedName, &after)).To(Succeed())
				Expect(after.Labels).ToNot(HaveKey(consts.RemoteUnavailableKey))
			})
		})

		When("the recovery is still within the stability window", func() {
			BeforeEach(func() {
				localPod.Labels = labels.Merge(localPod.Labels, labels.Set{consts.RemoteUnavailableKey: consts.RemoteUnavailableValue})
				buildClient(newLiqoNode(liqoNodeName, false), liqoNode2,
					newForeignCluster(liqov1beta1.ConditionStatusEstablished, now.Add(-10*time.Second)), localPod, localPod2)
			})

			It("should keep the label and requeue for the stability deadline", func() {
				result := reconcile(false)
				var after corev1.Pod
				Expect(fakeClient.Get(ctx, reqLocalPod.NamespacedName, &after)).To(Succeed())
				Expect(after.Labels).To(HaveKeyWithValue(consts.RemoteUnavailableKey, consts.RemoteUnavailableValue))
				Expect(result.RequeueAfter).To(BeNumerically(">", 0))
			})
		})
	})

	When("the peer state cannot be confirmed (missing ForeignCluster)", func() {
		BeforeEach(func() {
			buildClient(newLiqoNode(liqoNodeName, true), liqoNode2, localPod, localPod2)
		})

		It("should not guess, leaving the pod untouched", func() {
			reconcile(false)
			var after corev1.Pod
			Expect(fakeClient.Get(ctx, reqLocalPod.NamespacedName, &after)).To(Succeed())
			Expect(after.Labels).ToNot(HaveKey(consts.RemoteUnavailableKey))
		})
	})

	When("the node is cordoned or out of resources but the peer is connected", func() {
		BeforeEach(func() {
			node := newLiqoNode(liqoNodeName, false)
			node.Spec.Unschedulable = true
			node.Status.Allocatable = corev1.ResourceList{}
			buildClient(node, liqoNode2,
				newForeignCluster(liqov1beta1.ConditionStatusEstablished, now.Add(-5*time.Minute)), localPod, localPod2)
		})

		It("should not treat a cordon or resource exhaustion as a peer loss", func() {
			reconcile(false)
			var after corev1.Pod
			Expect(fakeClient.Get(ctx, reqLocalPod.NamespacedName, &after)).To(Succeed())
			Expect(after.Labels).ToNot(HaveKey(consts.RemoteUnavailableKey))
		})
	})
})

// boolToCondition maps a boolean to the corresponding Node condition status.
func boolToCondition(value bool) corev1.ConditionStatus {
	if value {
		return corev1.ConditionTrue
	}
	return corev1.ConditionFalse
}
