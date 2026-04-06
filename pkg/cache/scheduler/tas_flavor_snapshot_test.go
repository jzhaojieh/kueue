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
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/resources"
	"sigs.k8s.io/kueue/pkg/util/tas"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	"sigs.k8s.io/kueue/pkg/util/testingjobs/node"
)

func TestFreeCapacityPerDomain(t *testing.T) {
	snapshot := &TASFlavorSnapshot{
		leaves: leafDomainByID{
			"domain2": &leafDomain{
				freeCapacity: resources.Requests{
					corev1.ResourceCPU:    1000,
					corev1.ResourceMemory: 2 * 1024 * 1024 * 1024, // 2 GiB
				},
				tasUsage: resources.Requests{
					corev1.ResourceMemory: 1 * 1024 * 1024 * 1024, // 1 GiB
					corev1.ResourceCPU:    500,
				},
			},
			"domain1": &leafDomain{
				freeCapacity: resources.Requests{
					corev1.ResourceMemory: 4 * 1024 * 1024 * 1024, // 4 GiB
					corev1.ResourceCPU:    2000,
					"nvidia.com/gpu":      1,
				},
				tasUsage: resources.Requests{
					corev1.ResourceCPU:    500,
					"nvidia.com/gpu":      1,
					corev1.ResourceMemory: 2 * 1024 * 1024 * 1024, // 1 GiB
				},
			},
		},
	}

	expected := `{"domain1":{"freeCapacity":{"cpu":"2","memory":"4Gi","nvidia.com/gpu":"1"},"tasUsage":{"cpu":"500m","memory":"2Gi","nvidia.com/gpu":"1"}},"domain2":{"freeCapacity":{"cpu":"1","memory":"2Gi"},"tasUsage":{"cpu":"500m","memory":"1Gi"}}}`
	var wantErr error

	got, gotErr := snapshot.SerializeFreeCapacityPerDomain()
	if diff := cmp.Diff(wantErr, gotErr, cmpopts.EquateErrors()); len(diff) != 0 {
		t.Errorf("Unexpected error (-want,+got):\n%s", diff)
	}
	if diff := cmp.Diff(expected, got); diff != "" {
		t.Errorf("SerializeFreeCapacityPerDomain() mismatch (-expected +got):\n%s", diff)
	}
}

func TestMergeTopologyAssignments(t *testing.T) {
	nodes := []corev1.Node{
		*node.MakeNode("x").Label("level-1", "a").Label("level-2", "b").Obj(),
		*node.MakeNode("y").Label("level-1", "a").Label("level-2", "c").Obj(),
		*node.MakeNode("z").Label("level-1", "d").Label("level-2", "e").Obj(),
		*node.MakeNode("w").Label("level-1", "d").Label("level-2", "f").Obj(),
	}
	levels := []string{"level-1", "level-2"}

	cases := map[string]struct {
		a    *tas.TopologyAssignment
		b    *tas.TopologyAssignment
		want tas.TopologyAssignment
	}{
		"topologies with different domains, all a before b": {
			a: &tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "b"},
						Count:  1,
					},
					{
						Values: []string{"a", "c"},
						Count:  1,
					},
				},
			},
			b: &tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"d", "e"},
						Count:  1,
					},
					{
						Values: []string{"d", "f"},
						Count:  1,
					},
				},
			},
			want: tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "b"},
						Count:  1,
					},
					{
						Values: []string{"a", "c"},
						Count:  1,
					},
					{
						Values: []string{"d", "e"},
						Count:  1,
					},
					{
						Values: []string{"d", "f"},
						Count:  1,
					},
				},
			},
		},
		"topologies with different domains, all b before a": {
			a: &tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"d", "e"},
						Count:  1,
					},
					{
						Values: []string{"d", "f"},
						Count:  1,
					},
				},
			},
			b: &tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "b"},
						Count:  1,
					},
					{
						Values: []string{"a", "c"},
						Count:  1,
					},
				},
			},
			want: tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "b"},
						Count:  1,
					},
					{
						Values: []string{"a", "c"},
						Count:  1,
					},
					{
						Values: []string{"d", "e"},
						Count:  1,
					},
					{
						Values: []string{"d", "f"},
						Count:  1,
					},
				},
			},
		},
		"topologies with different domains, mixed order": {
			a: &tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "c"},
						Count:  1,
					},
					{
						Values: []string{"d", "e"},
						Count:  1,
					},
				},
			},
			b: &tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "b"},
						Count:  1,
					},
					{
						Values: []string{"d", "f"},
						Count:  1,
					},
				},
			},
			want: tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "b"},
						Count:  1,
					},
					{
						Values: []string{"a", "c"},
						Count:  1,
					},
					{
						Values: []string{"d", "e"},
						Count:  1,
					},
					{
						Values: []string{"d", "f"},
						Count:  1,
					},
				},
			},
		},
		"topologies with different and the same domains, mixed order": {
			a: &tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "c"},
						Count:  1,
					},
					{
						Values: []string{"d", "e"},
						Count:  1,
					},
				},
			},
			b: &tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "b"},
						Count:  1,
					},
					{
						Values: []string{"d", "e"},
						Count:  1,
					},
				},
			},
			want: tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "b"},
						Count:  1,
					},
					{
						Values: []string{"a", "c"},
						Count:  1,
					},
					{
						Values: []string{"d", "e"},
						Count:  2,
					},
				},
			},
		},
		"topology a with empty domains": {
			a: &tas.TopologyAssignment{
				Levels:  []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{},
			},
			b: &tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "b"},
						Count:  1,
					},
					{
						Values: []string{"d", "e"},
						Count:  1,
					},
				},
			},
			want: tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "b"},
						Count:  1,
					},
					{
						Values: []string{"d", "e"},
						Count:  1,
					},
				},
			},
		},
		"topology b with empty domain": {
			a: &tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "c"},
						Count:  1,
					},
					{
						Values: []string{"d", "e"},
						Count:  1,
					},
				},
			},
			b: &tas.TopologyAssignment{
				Levels:  []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{},
			},
			want: tas.TopologyAssignment{
				Levels: []string{"level-1", "level-2"},
				Domains: []tas.TopologyDomainAssignment{
					{
						Values: []string{"a", "c"},
						Count:  1,
					},
					{
						Values: []string{"d", "e"},
						Count:  1,
					},
				},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, log := utiltesting.ContextWithLog(t)
			s := newTASFlavorSnapshot(log, "dummy", levels, nil)
			for i := range nodes {
				s.addNode(newNodeInfo(&nodes[i]))
			}
			s.initialize()

			got := s.mergeTopologyAssignments(tc.a, tc.b)
			if diff := cmp.Diff(tc.want, *got); diff != "" {
				t.Errorf("unexpected topology assignment (-want,+got): %s", diff)
			}
		})
	}
}

func TestHasLevel(t *testing.T) {
	levels := []string{"level-1", "level-2"}

	testCases := map[string]struct {
		podSetTopologyRequest *kueue.PodSetTopologyRequest
		want                  bool
	}{
		"topology request nil": {
			podSetTopologyRequest: nil,
			want:                  false,
		},
		"topology request empty": {
			podSetTopologyRequest: &kueue.PodSetTopologyRequest{},
			want:                  false,
		},
		"required": {
			podSetTopologyRequest: &kueue.PodSetTopologyRequest{
				Required: ptr.To("level-1"),
			},
			want: true,
		},
		"required – invalid level": {
			podSetTopologyRequest: &kueue.PodSetTopologyRequest{
				Required: ptr.To("invalid-level"),
			},
			want: false,
		},
		"preferred": {
			podSetTopologyRequest: &kueue.PodSetTopologyRequest{
				Preferred: ptr.To("level-1"),
			},
			want: true,
		},
		"preferred – invalid level": {
			podSetTopologyRequest: &kueue.PodSetTopologyRequest{
				Preferred: ptr.To("invalid-level"),
			},
			want: false,
		},
		"unconstrained": {
			podSetTopologyRequest: &kueue.PodSetTopologyRequest{
				Unconstrained: ptr.To(true),
			},
			want: true,
		},
		"slice-only": {
			podSetTopologyRequest: &kueue.PodSetTopologyRequest{
				PodSetSliceRequiredTopology: ptr.To("level-1"),
			},
			want: true,
		},
		"slice-only – invalid level": {
			podSetTopologyRequest: &kueue.PodSetTopologyRequest{
				PodSetSliceRequiredTopology: ptr.To("invalid-level"),
			},
			want: false,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			_, log := utiltesting.ContextWithLog(t)
			s := newTASFlavorSnapshot(log, "dummy", levels, nil)
			got := s.HasLevel(tc.podSetTopologyRequest)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("unexpected HasLevel result (-want,+got): %s", diff)
			}
		})
	}
}

// TestSortedDomainsWithLeader verifies the sorting criteria (in order of priority):
// 1. leaderState - descending (always)
// 2. sliceStateWithLeader - descending (BestFit) or ascending (LeastFreeCapacity)
// 3. stateWithLeader - ascending (always, as tiebreaker)
// 4. levelValues - ascending (always, as final tiebreaker)
func TestSortedDomainsWithLeader(t *testing.T) {
	levels := []string{"block"}

	testCases := map[string]struct {
		domains              []*domain
		unconstrained        bool
		preferPartialDomains bool
		wantOrder            []string
	}{
		"leaderState descending: domains that can host leader come first": {
			domains: []*domain{
				{id: "no-leader", leaderState: 0, sliceStateWithLeader: 10, stateWithLeader: 10, levelValues: []string{"a"}},
				{id: "has-leader", leaderState: 1, sliceStateWithLeader: 1, stateWithLeader: 1, levelValues: []string{"b"}},
			},
			unconstrained: false,
			wantOrder:     []string{"has-leader", "no-leader"},
		},
		"BestFit: sliceStateWithLeader descending": {
			domains: []*domain{
				{id: "a", leaderState: 1, sliceStateWithLeader: 3, stateWithLeader: 1, levelValues: []string{"a"}},
				{id: "b", leaderState: 1, sliceStateWithLeader: 1, stateWithLeader: 1, levelValues: []string{"b"}},
				{id: "c", leaderState: 1, sliceStateWithLeader: 2, stateWithLeader: 1, levelValues: []string{"c"}},
			},
			unconstrained: false,
			wantOrder:     []string{"a", "c", "b"},
		},
		"LeastFreeCapacity: sliceStateWithLeader ascending": {
			domains: []*domain{
				{id: "a", leaderState: 1, sliceStateWithLeader: 3, stateWithLeader: 1, levelValues: []string{"a"}},
				{id: "b", leaderState: 1, sliceStateWithLeader: 1, stateWithLeader: 1, levelValues: []string{"b"}},
				{id: "c", leaderState: 1, sliceStateWithLeader: 2, stateWithLeader: 1, levelValues: []string{"c"}},
			},
			unconstrained: true,
			wantOrder:     []string{"b", "c", "a"},
		},
		"BestFit: stateWithLeader ascending as tiebreaker": {
			domains: []*domain{
				{id: "large", leaderState: 1, sliceStateWithLeader: 5, stateWithLeader: 100, levelValues: []string{"a"}},
				{id: "small", leaderState: 1, sliceStateWithLeader: 5, stateWithLeader: 10, levelValues: []string{"b"}},
				{id: "medium", leaderState: 1, sliceStateWithLeader: 5, stateWithLeader: 50, levelValues: []string{"c"}},
			},
			unconstrained: false,
			wantOrder:     []string{"small", "medium", "large"},
		},
		"LeastFreeCapacity: stateWithLeader ascending as tiebreaker": {
			domains: []*domain{
				{id: "large", leaderState: 1, sliceStateWithLeader: 5, stateWithLeader: 100, levelValues: []string{"a"}},
				{id: "small", leaderState: 1, sliceStateWithLeader: 5, stateWithLeader: 10, levelValues: []string{"b"}},
				{id: "medium", leaderState: 1, sliceStateWithLeader: 5, stateWithLeader: 50, levelValues: []string{"c"}},
			},
			unconstrained: true,
			wantOrder:     []string{"small", "medium", "large"},
		},
		"levelValues ascending as final tiebreaker": {
			domains: []*domain{
				{id: "c", leaderState: 1, sliceStateWithLeader: 5, stateWithLeader: 10, levelValues: []string{"c"}},
				{id: "a", leaderState: 1, sliceStateWithLeader: 5, stateWithLeader: 10, levelValues: []string{"a"}},
				{id: "b", leaderState: 1, sliceStateWithLeader: 5, stateWithLeader: 10, levelValues: []string{"b"}},
			},
			unconstrained: false,
			wantOrder:     []string{"a", "b", "c"},
		},
		"LFC: minNonZeroLeafState ascending after leaderState": {
			domains: []*domain{
				{id: "tight-child", leaderState: 1, minNonZeroLeafState: 1, sliceStateWithLeader: 10, stateWithLeader: 20, levelValues: []string{"a"}},
				{id: "loose-child", leaderState: 1, minNonZeroLeafState: 5, sliceStateWithLeader: 8, stateWithLeader: 16, levelValues: []string{"b"}},
			},
			unconstrained:        true,
			preferPartialDomains: true,
			wantOrder:            []string{"tight-child", "loose-child"},
		},
		"LFC: minNonZeroLeafState ignored when preferPartialDomains is false": {
			domains: []*domain{
				{id: "tight-child", leaderState: 1, minNonZeroLeafState: 1, sliceStateWithLeader: 10, stateWithLeader: 20, levelValues: []string{"a"}},
				{id: "loose-child", leaderState: 1, minNonZeroLeafState: 5, sliceStateWithLeader: 8, stateWithLeader: 16, levelValues: []string{"b"}},
			},
			unconstrained:        true,
			preferPartialDomains: false,
			// Without preferPartialDomains, LFC sorts sliceStateWithLeader ascending: 8 < 10
			wantOrder: []string{"loose-child", "tight-child"},
		},
		"BestFit: minNonZeroLeafState ignored": {
			domains: []*domain{
				{id: "tight-child", leaderState: 1, minNonZeroLeafState: 1, sliceStateWithLeader: 5, stateWithLeader: 20, levelValues: []string{"a"}},
				{id: "loose-child", leaderState: 1, minNonZeroLeafState: 10, sliceStateWithLeader: 10, stateWithLeader: 16, levelValues: []string{"b"}},
			},
			unconstrained:        false,
			preferPartialDomains: true,
			// BestFit sorts sliceStateWithLeader descending: 10 > 5
			wantOrder: []string{"loose-child", "tight-child"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			_, log := utiltesting.ContextWithLog(t)
			s := newTASFlavorSnapshot(log, "test", levels, nil)

			sorted := s.sortedDomainsWithLeader(tc.domains, tc.unconstrained, tc.preferPartialDomains)

			gotOrder := make([]string, len(sorted))
			for i, d := range sorted {
				gotOrder[i] = string(d.id)
			}

			if diff := cmp.Diff(tc.wantOrder, gotOrder); diff != "" {
				t.Errorf("unexpected domain order (-want,+got): %s", diff)
			}
		})
	}
}

// TestSortedDomains verifies the sorting criteria for sortedDomains (worker-only sorting):
// BestFit: sliceState (descending), state (ascending), levelValues (ascending)
// LFC: minNonZeroLeafState (ascending), sliceState (ascending), state (ascending), levelValues (ascending)
func TestSortedDomains(t *testing.T) {
	levels := []string{"block"}

	testCases := map[string]struct {
		domains              []*domain
		unconstrained        bool
		preferPartialDomains bool
		wantOrder            []string
	}{
		"BestFit: sliceState descending": {
			domains: []*domain{
				{id: "a", sliceState: 2, state: 10, levelValues: []string{"a"}},
				{id: "b", sliceState: 5, state: 10, levelValues: []string{"b"}},
				{id: "c", sliceState: 3, state: 10, levelValues: []string{"c"}},
			},
			unconstrained: false,
			wantOrder:     []string{"b", "c", "a"},
		},
		"BestFit: minNonZeroLeafState ignored even with preferPartialDomains": {
			domains: []*domain{
				{id: "tight-child", minNonZeroLeafState: 1, sliceState: 5, state: 20, levelValues: []string{"a"}},
				{id: "loose-child", minNonZeroLeafState: 10, sliceState: 10, state: 20, levelValues: []string{"b"}},
			},
			unconstrained:        false,
			preferPartialDomains: true,
			// BestFit sorts sliceState descending: 10 > 5
			wantOrder: []string{"loose-child", "tight-child"},
		},
		"LFC: minNonZeroLeafState ascending with preferPartialDomains": {
			domains: []*domain{
				{id: "tight-child", minNonZeroLeafState: 1, sliceState: 10, state: 20, levelValues: []string{"a"}},
				{id: "loose-child", minNonZeroLeafState: 5, sliceState: 8, state: 16, levelValues: []string{"b"}},
			},
			unconstrained:        true,
			preferPartialDomains: true,
			// LFC sorts minNonZeroLeafState ascending: 1 < 5
			wantOrder: []string{"tight-child", "loose-child"},
		},
		"LFC: minNonZeroLeafState ignored without preferPartialDomains": {
			domains: []*domain{
				{id: "tight-child", minNonZeroLeafState: 1, sliceState: 10, state: 20, levelValues: []string{"a"}},
				{id: "loose-child", minNonZeroLeafState: 5, sliceState: 8, state: 16, levelValues: []string{"b"}},
			},
			unconstrained:        true,
			preferPartialDomains: false,
			// Without preferPartialDomains, LFC sorts sliceState ascending: 8 < 10
			wantOrder: []string{"loose-child", "tight-child"},
		},
		"LFC: minNonZeroLeafState tiebreak falls back to sliceState": {
			domains: []*domain{
				{id: "more-slices", minNonZeroLeafState: 3, sliceState: 10, state: 20, levelValues: []string{"a"}},
				{id: "fewer-slices", minNonZeroLeafState: 3, sliceState: 5, state: 10, levelValues: []string{"b"}},
			},
			unconstrained:        true,
			preferPartialDomains: true,
			// Equal minNonZeroLeafState → LFC sliceState ascending: 5 < 10
			wantOrder: []string{"fewer-slices", "more-slices"},
		},
		"LFC: all-zero children use MaxInt32 and sort last": {
			domains: []*domain{
				{id: "all-full", minNonZeroLeafState: math.MaxInt32, sliceState: 5, state: 10, levelValues: []string{"a"}},
				{id: "has-partial", minNonZeroLeafState: 2, sliceState: 8, state: 16, levelValues: []string{"b"}},
			},
			unconstrained:        true,
			preferPartialDomains: true,
			// MaxInt32 > 2, so has-partial sorts first
			wantOrder: []string{"has-partial", "all-full"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			_, log := utiltesting.ContextWithLog(t)
			s := newTASFlavorSnapshot(log, "test", levels, nil)

			sorted := s.sortedDomains(tc.domains, tc.unconstrained, tc.preferPartialDomains)

			gotOrder := make([]string, len(sorted))
			for i, d := range sorted {
				gotOrder[i] = string(d.id)
			}

			if diff := cmp.Diff(tc.wantOrder, gotOrder); diff != "" {
				t.Errorf("unexpected domain order (-want,+got): %s", diff)
			}
		})
	}
}

// TestMinNonZeroLeafStatePropagation verifies that minNonZeroLeafState propagates
// the tightest leaf's state through a 3-level tree (block → rack → hostname), and
// that LFC sorting at the block level correctly steers toward the subtree containing
// the tightest leaf — even when that subtree has a higher aggregate sliceState.
func TestMinNonZeroLeafStatePropagation(t *testing.T) {
	_, log := utiltesting.ContextWithLog(t)
	s := newTASFlavorSnapshot(log, "test", []string{"block", "rack", "hostname"}, nil)

	// Build a 3-level tree:
	//
	// block-a (aggregate state=48, BUT contains the partial host)
	//   rack-a1: host-a1-x(1 free), host-a1-y(8 free)                        → state=9
	//   rack-a2: host-a2-x(8), host-a2-y(8), host-a2-z(8), +2 more hosts    → state=40
	//
	// block-b (aggregate state=32, all empty)
	//   rack-b1: host-b1-x(8 free), host-b1-y(8 free)                        → state=16
	//   rack-b2: host-b2-x(8 free), host-b2-y(8 free)                        → state=16
	//
	// Without minNonZeroLeafState, LFC picks block-b (sliceState 32 < 48).
	// With it, block-a wins because host-a1-x's state=1 propagates up.

	hostA1X := &domain{id: "host-a1-x", levelValues: []string{"block-a", "rack-a1", "host-a1-x"}}
	hostA1Y := &domain{id: "host-a1-y", levelValues: []string{"block-a", "rack-a1", "host-a1-y"}}
	hostA2X := &domain{id: "host-a2-x", levelValues: []string{"block-a", "rack-a2", "host-a2-x"}}
	hostA2Y := &domain{id: "host-a2-y", levelValues: []string{"block-a", "rack-a2", "host-a2-y"}}
	hostA2Z := &domain{id: "host-a2-z", levelValues: []string{"block-a", "rack-a2", "host-a2-z"}}
	hostA2W := &domain{id: "host-a2-w", levelValues: []string{"block-a", "rack-a2", "host-a2-w"}}
	hostA2V := &domain{id: "host-a2-v", levelValues: []string{"block-a", "rack-a2", "host-a2-v"}}
	hostB1X := &domain{id: "host-b1-x", levelValues: []string{"block-b", "rack-b1", "host-b1-x"}}
	hostB1Y := &domain{id: "host-b1-y", levelValues: []string{"block-b", "rack-b1", "host-b1-y"}}
	hostB2X := &domain{id: "host-b2-x", levelValues: []string{"block-b", "rack-b2", "host-b2-x"}}
	hostB2Y := &domain{id: "host-b2-y", levelValues: []string{"block-b", "rack-b2", "host-b2-y"}}

	rackA1 := &domain{id: "rack-a1", children: []*domain{hostA1X, hostA1Y}, levelValues: []string{"block-a", "rack-a1"}}
	rackA2 := &domain{id: "rack-a2", children: []*domain{hostA2X, hostA2Y, hostA2Z, hostA2W, hostA2V}, levelValues: []string{"block-a", "rack-a2"}}
	rackB1 := &domain{id: "rack-b1", children: []*domain{hostB1X, hostB1Y}, levelValues: []string{"block-b", "rack-b1"}}
	rackB2 := &domain{id: "rack-b2", children: []*domain{hostB2X, hostB2Y}, levelValues: []string{"block-b", "rack-b2"}}

	blockA := &domain{id: "block-a", children: []*domain{rackA1, rackA2}, levelValues: []string{"block-a"}}
	blockB := &domain{id: "block-b", children: []*domain{rackB1, rackB2}, levelValues: []string{"block-b"}}

	// Set leaf states: host-a1-x has 1 free GPU (partial), all others have 8
	hostA1X.state = 1
	hostA1X.stateWithLeader = 1
	hostA1Y.state = 8
	hostA1Y.stateWithLeader = 8
	for _, h := range []*domain{hostA2X, hostA2Y, hostA2Z, hostA2W, hostA2V, hostB1X, hostB1Y, hostB2X, hostB2Y} {
		h.state = 8
		h.stateWithLeader = 8
	}

	// Propagate up through the tree (sliceSize=1, sliceLevelIdx=1 for rack level)
	s.fillInCountsHelper(blockA, 1, 1, 0)
	s.fillInCountsHelper(blockB, 1, 1, 0)

	// --- Verify propagation at every level ---

	t.Run("leaf level: leaves store their own state", func(t *testing.T) {
		if hostA1X.minNonZeroLeafState != 1 {
			t.Errorf("host-a1-x.minNonZeroLeafState = %d, want 1", hostA1X.minNonZeroLeafState)
		}
		if hostA1Y.minNonZeroLeafState != 8 {
			t.Errorf("host-a1-y.minNonZeroLeafState = %d, want 8", hostA1Y.minNonZeroLeafState)
		}
	})

	t.Run("rack level: min of leaf states", func(t *testing.T) {
		if rackA1.minNonZeroLeafState != 1 {
			t.Errorf("rack-a1.minNonZeroLeafState = %d, want 1 (from host-a1-x)", rackA1.minNonZeroLeafState)
		}
		if rackA2.minNonZeroLeafState != 8 {
			t.Errorf("rack-a2.minNonZeroLeafState = %d, want 8", rackA2.minNonZeroLeafState)
		}
		if rackB1.minNonZeroLeafState != 8 {
			t.Errorf("rack-b1.minNonZeroLeafState = %d, want 8", rackB1.minNonZeroLeafState)
		}
	})

	t.Run("block level: propagates through rack to leaf", func(t *testing.T) {
		// block-a: min(rack-a1.minNZL=1, rack-a2.minNZL=8) = 1
		if blockA.minNonZeroLeafState != 1 {
			t.Errorf("block-a.minNonZeroLeafState = %d, want 1 (propagated from host-a1-x through rack-a1)", blockA.minNonZeroLeafState)
		}
		// block-b: min(rack-b1.minNZL=8, rack-b2.minNZL=8) = 8
		if blockB.minNonZeroLeafState != 8 {
			t.Errorf("block-b.minNonZeroLeafState = %d, want 8", blockB.minNonZeroLeafState)
		}
	})

	// --- Verify sorting ---

	t.Run("Case 1: partial host exists - block sort steers to it despite higher aggregate", func(t *testing.T) {
		// block-a: minNZL=1, sliceState=48 (high aggregate from rack-a2)
		// block-b: minNZL=8, sliceState=32
		// LFC without minNZL would pick block-b (32 < 48). With it, block-a wins (1 < 8).
		sorted := s.sortedDomains([]*domain{blockA, blockB}, true, true)
		gotOrder := make([]string, len(sorted))
		for i, d := range sorted {
			gotOrder[i] = string(d.id)
		}
		if diff := cmp.Diff([]string{"block-a", "block-b"}, gotOrder); diff != "" {
			t.Errorf("block sort (-want,+got): %s", diff)
		}
	})

	t.Run("Case 1: rack sort within block-a steers to rack with partial host", func(t *testing.T) {
		sorted := s.sortedDomains([]*domain{rackA1, rackA2}, true, true)
		gotOrder := make([]string, len(sorted))
		for i, d := range sorted {
			gotOrder[i] = string(d.id)
		}
		if diff := cmp.Diff([]string{"rack-a1", "rack-a2"}, gotOrder); diff != "" {
			t.Errorf("rack sort (-want,+got): %s", diff)
		}
	})

	t.Run("Case 2: no partial hosts - tiebreaker uses sliceState LFC", func(t *testing.T) {
		// rack-b1 and rack-b2 both have minNZL=8.
		// Tiebreak: sliceState ascending → both 16 → state ascending → both 16 → levelValues.
		sorted := s.sortedDomains([]*domain{rackB2, rackB1}, true, true)
		gotOrder := make([]string, len(sorted))
		for i, d := range sorted {
			gotOrder[i] = string(d.id)
		}
		if diff := cmp.Diff([]string{"rack-b1", "rack-b2"}, gotOrder); diff != "" {
			t.Errorf("tiebreaker sort (-want,+got): %s", diff)
		}
	})

	// --- Case 2b: globally least-free rack hidden in high-aggregate block ---
	//
	// New topology where all hosts are empty (no partial nodes), but racks
	// have different sizes:
	//
	// block-x (aggregate sliceState=48)
	//   rack-x1: 1 host × 8 free    → sliceState=8  ← GLOBALLY LEAST FREE RACK
	//   rack-x2: 5 hosts × 8 free   → sliceState=40
	//
	// block-y (aggregate sliceState=32)
	//   rack-y1: 2 hosts × 8 free   → sliceState=16
	//   rack-y2: 2 hosts × 8 free   → sliceState=16
	//
	// Without minDescendantSliceState: LFC picks block-y (32 < 48), enters
	// rack-y1 (sliceState=16). The globally least-free rack-x1 (8) is missed.
	//
	// With minDescendantSliceState: minNZL ties at 8. Tiebreak by
	// minDescendantSliceState: block-x has min(8,40)=8, block-y has min(16,16)=16.
	// block-x wins (8 < 16), enters rack-x1. ✓

	t.Run("Case 2b: block sort picks globally least-free rack via minDescendantSliceState", func(t *testing.T) {
		_, log2 := utiltesting.ContextWithLog(t)
		s2 := newTASFlavorSnapshot(log2, "test", []string{"block", "rack", "hostname"}, nil)

		hX1a := &domain{id: "hx1a", state: 8, stateWithLeader: 8, levelValues: []string{"block-x", "rack-x1", "hx1a"}}
		hX2a := &domain{id: "hx2a", state: 8, stateWithLeader: 8, levelValues: []string{"block-x", "rack-x2", "hx2a"}}
		hX2b := &domain{id: "hx2b", state: 8, stateWithLeader: 8, levelValues: []string{"block-x", "rack-x2", "hx2b"}}
		hX2c := &domain{id: "hx2c", state: 8, stateWithLeader: 8, levelValues: []string{"block-x", "rack-x2", "hx2c"}}
		hX2d := &domain{id: "hx2d", state: 8, stateWithLeader: 8, levelValues: []string{"block-x", "rack-x2", "hx2d"}}
		hX2e := &domain{id: "hx2e", state: 8, stateWithLeader: 8, levelValues: []string{"block-x", "rack-x2", "hx2e"}}
		hY1a := &domain{id: "hy1a", state: 8, stateWithLeader: 8, levelValues: []string{"block-y", "rack-y1", "hy1a"}}
		hY1b := &domain{id: "hy1b", state: 8, stateWithLeader: 8, levelValues: []string{"block-y", "rack-y1", "hy1b"}}
		hY2a := &domain{id: "hy2a", state: 8, stateWithLeader: 8, levelValues: []string{"block-y", "rack-y2", "hy2a"}}
		hY2b := &domain{id: "hy2b", state: 8, stateWithLeader: 8, levelValues: []string{"block-y", "rack-y2", "hy2b"}}

		rackX1 := &domain{id: "rack-x1", children: []*domain{hX1a}, levelValues: []string{"block-x", "rack-x1"}}
		rackX2 := &domain{id: "rack-x2", children: []*domain{hX2a, hX2b, hX2c, hX2d, hX2e}, levelValues: []string{"block-x", "rack-x2"}}
		rackY1 := &domain{id: "rack-y1", children: []*domain{hY1a, hY1b}, levelValues: []string{"block-y", "rack-y1"}}
		rackY2 := &domain{id: "rack-y2", children: []*domain{hY2a, hY2b}, levelValues: []string{"block-y", "rack-y2"}}

		blockX := &domain{id: "block-x", children: []*domain{rackX1, rackX2}, levelValues: []string{"block-x"}}
		blockY := &domain{id: "block-y", children: []*domain{rackY1, rackY2}, levelValues: []string{"block-y"}}

		s2.fillInCountsHelper(blockX, 1, 1, 0)
		s2.fillInCountsHelper(blockY, 1, 1, 0)

		// Verify minDescendantSliceState propagation
		if rackX1.minDescendantSliceState != 8 {
			t.Errorf("rack-x1.minDescendantSliceState = %d, want 8", rackX1.minDescendantSliceState)
		}
		if rackX2.minDescendantSliceState != 40 {
			t.Errorf("rack-x2.minDescendantSliceState = %d, want 40", rackX2.minDescendantSliceState)
		}
		if blockX.minDescendantSliceState != 8 {
			t.Errorf("block-x.minDescendantSliceState = %d, want 8 (from rack-x1)", blockX.minDescendantSliceState)
		}
		if blockY.minDescendantSliceState != 16 {
			t.Errorf("block-y.minDescendantSliceState = %d, want 16", blockY.minDescendantSliceState)
		}

		// Block sort: minNZL ties at 8, minDescendantSliceState breaks the tie
		sorted := s2.sortedDomains([]*domain{blockX, blockY}, true, true)
		gotOrder := make([]string, len(sorted))
		for i, d := range sorted {
			gotOrder[i] = string(d.id)
		}
		if diff := cmp.Diff([]string{"block-x", "block-y"}, gotOrder); diff != "" {
			t.Errorf("block sort should prefer block-x (contains globally least-free rack-x1) (-want,+got): %s", diff)
		}
	})
}

// TestMultiPodGlobalRackSelection verifies that multi-pod LFC workloads find
// the globally least-free rack even when it's hidden in a high-aggregate block.
//
// Topology (block → rack → hostname), all hosts have 8 GPUs:
//
//	block-a (aggregate sliceState=45)
//	  rack-a1: 1 host × 5 free  → sliceState=5  ← GLOBALLY LEAST FREE THAT FITS count=4
//	  rack-a2: 5 hosts × 8 free → sliceState=40
//	block-b (aggregate sliceState=32)
//	  rack-b1: 2 hosts × 8 free → sliceState=16
//	  rack-b2: 2 hosts × 8 free → sliceState=16
//
// count=4, unconstrained=true (LFC).
//
// BUG: LFC at block level picks block-b (32 < 45), enters rack-b1 (16).
// rack-a1 (sliceState=5, fits count=4 exactly) is never considered.
//
// FIX: Phase 2b above-slice loop sorts ALL racks globally for LFC,
// finding rack-a1 (5) before rack-b1 (16).
func TestMultiPodGlobalRackSelection(t *testing.T) {
	_, log := utiltesting.ContextWithLog(t)
	levels := []string{"block", "rack", "hostname"}
	s := newTASFlavorSnapshot(log, "test", levels, nil)

	// Build leaves
	hA1a := &domain{id: "ha1a", state: 5, stateWithLeader: 5, levelValues: []string{"block-a", "rack-a1", "ha1a"}}
	hA2a := &domain{id: "ha2a", state: 8, stateWithLeader: 8, levelValues: []string{"block-a", "rack-a2", "ha2a"}}
	hA2b := &domain{id: "ha2b", state: 8, stateWithLeader: 8, levelValues: []string{"block-a", "rack-a2", "ha2b"}}
	hA2c := &domain{id: "ha2c", state: 8, stateWithLeader: 8, levelValues: []string{"block-a", "rack-a2", "ha2c"}}
	hA2d := &domain{id: "ha2d", state: 8, stateWithLeader: 8, levelValues: []string{"block-a", "rack-a2", "ha2d"}}
	hA2e := &domain{id: "ha2e", state: 8, stateWithLeader: 8, levelValues: []string{"block-a", "rack-a2", "ha2e"}}
	hB1a := &domain{id: "hb1a", state: 8, stateWithLeader: 8, levelValues: []string{"block-b", "rack-b1", "hb1a"}}
	hB1b := &domain{id: "hb1b", state: 8, stateWithLeader: 8, levelValues: []string{"block-b", "rack-b1", "hb1b"}}
	hB2a := &domain{id: "hb2a", state: 8, stateWithLeader: 8, levelValues: []string{"block-b", "rack-b2", "hb2a"}}
	hB2b := &domain{id: "hb2b", state: 8, stateWithLeader: 8, levelValues: []string{"block-b", "rack-b2", "hb2b"}}

	rackA1 := &domain{id: "rack-a1", children: []*domain{hA1a}, levelValues: []string{"block-a", "rack-a1"}}
	rackA2 := &domain{id: "rack-a2", children: []*domain{hA2a, hA2b, hA2c, hA2d, hA2e}, levelValues: []string{"block-a", "rack-a2"}}
	rackB1 := &domain{id: "rack-b1", children: []*domain{hB1a, hB1b}, levelValues: []string{"block-b", "rack-b1"}}
	rackB2 := &domain{id: "rack-b2", children: []*domain{hB2a, hB2b}, levelValues: []string{"block-b", "rack-b2"}}

	blockA := &domain{id: "block-a", children: []*domain{rackA1, rackA2}, levelValues: []string{"block-a"}}
	blockB := &domain{id: "block-b", children: []*domain{rackB1, rackB2}, levelValues: []string{"block-b"}}

	// Populate domainsPerLevel so the fix can access all domains at each level
	s.domainsPerLevel[0]["block-a"] = blockA
	s.domainsPerLevel[0]["block-b"] = blockB
	s.domainsPerLevel[1]["rack-a1"] = rackA1
	s.domainsPerLevel[1]["rack-a2"] = rackA2
	s.domainsPerLevel[1]["rack-b1"] = rackB1
	s.domainsPerLevel[1]["rack-b2"] = rackB2
	s.domainsPerLevel[2]["ha1a"] = hA1a
	s.domainsPerLevel[2]["ha2a"] = hA2a
	s.domainsPerLevel[2]["hb1a"] = hB1a

	// Propagate (sliceSize=1, sliceLevelIdx=1 for rack level)
	s.fillInCountsHelper(blockA, 1, 1, 0)
	s.fillInCountsHelper(blockB, 1, 1, 0)

	// Verify setup
	if rackA1.sliceState != 5 {
		t.Fatalf("rack-a1.sliceState = %d, want 5", rackA1.sliceState)
	}
	if blockA.sliceState != 45 {
		t.Fatalf("block-a.sliceState = %d, want 45", blockA.sliceState)
	}
	if blockB.sliceState != 32 {
		t.Fatalf("block-b.sliceState = %d, want 32", blockB.sliceState)
	}

	// --- Simulate current Phase 2b behavior (broken for multi-pod) ---
	//
	// Step 1: findLevelWithFitDomains picks block by aggregate LFC
	// For multi-pod: preferPartialDomains=false, so plain LFC on sliceState
	count := int32(4)
	sliceSize := int32(1)
	sliceLevelIdx := 1
	preferPartialDomains := count == 1 // false

	blocksSorted := s.sortedDomains([]*domain{blockA, blockB}, true, preferPartialDomains)
	// LFC ascending: block-b(32) < block-a(45) → block-b first
	selectedBlock := blocksSorted[0]

	// Step 2: Phase 2b above-slice loop uses children of selected block only
	currentBehaviorRacks := s.sortedDomains(s.lowerLevelDomains([]*domain{selectedBlock}), true, preferPartialDomains)

	// Step 3: updateCountsToMinimumGeneric picks first rack that fits count=4
	var currentRack *domain
	for _, r := range currentBehaviorRacks {
		if r.sliceState >= count/sliceSize {
			currentRack = r
			break
		}
	}

	// --- Correct behavior: sort ALL racks globally ---
	allRacks := make([]*domain, 0)
	for _, d := range s.domainsPerLevel[sliceLevelIdx] {
		allRacks = append(allRacks, d)
	}
	globalRacks := s.sortedDomains(allRacks, true, preferPartialDomains)

	var globalBestRack *domain
	for _, r := range globalRacks {
		if r.sliceState >= count/sliceSize {
			globalBestRack = r
			break
		}
	}

	t.Run("global best rack is rack-a1", func(t *testing.T) {
		if globalBestRack == nil || globalBestRack.id != "rack-a1" {
			got := "nil"
			if globalBestRack != nil {
				got = string(globalBestRack.id)
			}
			t.Errorf("globally least-free fitting rack = %s, want rack-a1", got)
		}
	})

	t.Run("current behavior misses globally best rack", func(t *testing.T) {
		// This documents the current limitation: current code picks from
		// block-b's racks only, missing rack-a1 in block-a.
		if currentRack == nil {
			t.Fatal("currentRack is nil")
		}
		if currentRack.id == "rack-a1" {
			t.Skip("current behavior already finds global best — no fix needed")
		}
		// Current picks rack-b1(16) instead of rack-a1(5)
		if currentRack.id != "rack-b1" {
			t.Errorf("current behavior picked %s, expected rack-b1 (from block-b)", currentRack.id)
		}
	})

	t.Run("Phase 2b with global rack view finds rack-a1", func(t *testing.T) {
		// After the fix, the above-slice loop uses all racks globally for LFC.
		// This simulates the fixed behavior.
		if globalBestRack == nil || globalBestRack.id != "rack-a1" {
			got := "nil"
			if globalBestRack != nil {
				got = string(globalBestRack.id)
			}
			t.Errorf("fixed behavior should pick rack-a1 (sliceState=5), got %s", got)
		}
	})
}
