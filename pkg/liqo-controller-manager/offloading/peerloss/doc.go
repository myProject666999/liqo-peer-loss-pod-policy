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

// Package peerloss defines the policy applied to local offloaded Pods when the network
// connection towards the peer cluster they are offloaded to is lost.
//
// The package only contains pure decision logic: given the current time, the observed node state,
// the peer connectivity, the Pod controller type and an explicit policy, it returns the action the
// controller is allowed to perform and the next instant at which the decision should be re-evaluated.
// The controller is responsible for executing the action and for scheduling the re-evaluation.
//
// The policy is intentionally conservative. Liqo cannot prove, from the consumer cluster, that the
// Pods running on an unreachable provider have actually stopped. Recreating them while the remote
// copies are still alive could produce two active replicas (dual-active), which is unsafe for workloads
// that do not tolerate concurrent writers. Therefore Pods are only marked as remotely unavailable and
// retained unless eviction is explicitly authorized through the Pod annotation (and the global switch).
package peerloss
