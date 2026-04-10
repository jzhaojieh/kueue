/*
Copyright The Kubernetes Authors.

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

package scheduler

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/kueue/pkg/resources"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
)

// verifyNodeUsageConsistency recomputes usage from podUsage (old O(pods) algorithm)
// and verifies it matches the incremental nodeUsage.
func verifyNodeUsageConsistency(t *testing.T, cache *nonTasUsageCache) {
	t.Helper()
	expected := make(map[string]resources.Requests)
	for _, pv := range cache.podUsage {
		if _, found := expected[pv.node]; !found {
			expected[pv.node] = resources.Requests{}
		}
		expected[pv.node].Add(pv.usage)
		expected[pv.node][corev1.ResourcePods]++
	}
	got := cache.usagePerNode()
	if diff := cmp.Diff(expected, got); diff != "" {
		t.Errorf("nodeUsage inconsistent with podUsage (-recomputed +nodeUsage):\n%s", diff)
	}
}

func makePod(name, namespace, node string, cpu string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PodSpec{
			NodeName: node,
			Containers: []corev1.Container{{
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse(cpu),
					},
				},
			}},
		},
	}
}

func TestNonTasUsageCacheIncrementalUpdates(t *testing.T) {
	newCache := func() *nonTasUsageCache {
		return &nonTasUsageCache{
			podUsage:  make(map[types.NamespacedName]podUsageValue),
			nodeUsage: make(map[string]resources.Requests),
		}
	}

	t.Run("add then delete same pod", func(t *testing.T) {
		_, log := utiltesting.ContextWithLog(t)
		cache := newCache()

		pod := makePod("pod1", "ns", "node-a", "2")
		cache.update(pod, log)
		verifyNodeUsageConsistency(t, cache)

		cache.delete(client.ObjectKeyFromObject(pod), log)
		verifyNodeUsageConsistency(t, cache)

		got := cache.usagePerNode()
		if len(got) != 0 {
			t.Errorf("expected empty usage after delete, got %v", got)
		}
	})

	t.Run("multiple pods on same node", func(t *testing.T) {
		_, log := utiltesting.ContextWithLog(t)
		cache := newCache()

		cache.update(makePod("pod1", "ns", "node-a", "1"), log)
		cache.update(makePod("pod2", "ns", "node-a", "2"), log)
		verifyNodeUsageConsistency(t, cache)

		got := cache.usagePerNode()
		if got["node-a"][corev1.ResourceCPU] != 3000 {
			t.Errorf("CPU = %d, want 3000", got["node-a"][corev1.ResourceCPU])
		}
		if got["node-a"][corev1.ResourcePods] != 2 {
			t.Errorf("Pods = %d, want 2", got["node-a"][corev1.ResourcePods])
		}
	})

	t.Run("pod moves between nodes", func(t *testing.T) {
		_, log := utiltesting.ContextWithLog(t)
		cache := newCache()

		pod := makePod("pod1", "ns", "node-a", "4")
		cache.update(pod, log)

		// Pod moves to node-b
		pod.Spec.NodeName = "node-b"
		cache.update(pod, log)
		verifyNodeUsageConsistency(t, cache)

		got := cache.usagePerNode()
		if _, found := got["node-a"]; found {
			t.Error("node-a should have been cleaned up after pod moved")
		}
		if got["node-b"][corev1.ResourceCPU] != 4000 {
			t.Errorf("node-b CPU = %d, want 4000", got["node-b"][corev1.ResourceCPU])
		}
	})

	t.Run("terminated pod removes usage", func(t *testing.T) {
		_, log := utiltesting.ContextWithLog(t)
		cache := newCache()

		pod := makePod("pod1", "ns", "node-a", "2")
		cache.update(pod, log)

		// Terminate the pod
		pod.Status.Phase = corev1.PodSucceeded
		cache.update(pod, log)
		verifyNodeUsageConsistency(t, cache)

		got := cache.usagePerNode()
		if len(got) != 0 {
			t.Errorf("expected empty after termination, got %v", got)
		}
	})

	t.Run("delete non-existent key is no-op", func(t *testing.T) {
		_, log := utiltesting.ContextWithLog(t)
		cache := newCache()
		// Should not panic
		cache.delete(client.ObjectKey{Namespace: "ns", Name: "ghost"}, log)
		verifyNodeUsageConsistency(t, cache)
	})

	t.Run("last pod removed cleans up node entry", func(t *testing.T) {
		_, log := utiltesting.ContextWithLog(t)
		cache := newCache()

		cache.update(makePod("pod1", "ns", "node-a", "1"), log)
		cache.update(makePod("pod2", "ns", "node-a", "1"), log)
		cache.delete(client.ObjectKey{Namespace: "ns", Name: "pod1"}, log)
		verifyNodeUsageConsistency(t, cache)

		got := cache.usagePerNode()
		if got["node-a"][corev1.ResourcePods] != 1 {
			t.Errorf("Pods = %d, want 1 after removing one of two", got["node-a"][corev1.ResourcePods])
		}

		cache.delete(client.ObjectKey{Namespace: "ns", Name: "pod2"}, log)
		verifyNodeUsageConsistency(t, cache)

		got = cache.usagePerNode()
		if _, found := got["node-a"]; found {
			t.Error("node-a should be cleaned up when last pod removed")
		}
	})

	t.Run("usagePerNode returns deep copy", func(t *testing.T) {
		_, log := utiltesting.ContextWithLog(t)
		cache := newCache()

		cache.update(makePod("pod1", "ns", "node-a", "4"), log)
		got := cache.usagePerNode()

		// Mutate the returned map
		got["node-a"][corev1.ResourceCPU] = 9999

		// Internal state should be unaffected
		got2 := cache.usagePerNode()
		if got2["node-a"][corev1.ResourceCPU] != 4000 {
			t.Errorf("internal state was mutated: CPU = %d, want 4000", got2["node-a"][corev1.ResourceCPU])
		}
	})
}
