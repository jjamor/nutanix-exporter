/*
Copyright © 2024 Ingka Holding B.V. All Rights Reserved.
Licensed under the GPL, Version 2 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

       <https://www.gnu.org/licenses/gpl-2.0.en.html>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package collector

import (
	"testing"
)

func Test_parseVMv3Entity(t *testing.T) {
	tests := []struct {
		name    string
		entity  any
		wantNil bool
		want    vmV3Info
	}{
		{
			name: "valid entity with all fields",
			entity: map[string]any{
				"metadata": map[string]any{
					"uuid": "abc-123",
				},
				"status": map[string]any{
					"name": "test-vm",
					"cluster_reference": map[string]any{
						"name": "cluster-01",
					},
					"resources": map[string]any{
						"power_state":     "ON",
						"hypervisor_type": "AHV",
					},
				},
			},
			wantNil: false,
			want: vmV3Info{
				name:           "test-vm",
				uuid:           "abc-123",
				clusterName:    "cluster-01",
				powerState:     "ON",
				hypervisorType: "AHV",
			},
		},
		{
			name: "powered off VM",
			entity: map[string]any{
				"metadata": map[string]any{
					"uuid": "def-456",
				},
				"status": map[string]any{
					"name": "stopped-vm",
					"cluster_reference": map[string]any{
						"name": "cluster-02",
					},
					"resources": map[string]any{
						"power_state":     "OFF",
						"hypervisor_type": "AHV",
					},
				},
			},
			wantNil: false,
			want: vmV3Info{
				name:           "stopped-vm",
				uuid:           "def-456",
				clusterName:    "cluster-02",
				powerState:     "OFF",
				hypervisorType: "AHV",
			},
		},
		{
			name: "missing status",
			entity: map[string]any{
				"metadata": map[string]any{
					"uuid": "ghi-789",
				},
			},
			wantNil: true,
		},
		{
			name: "empty name",
			entity: map[string]any{
				"metadata": map[string]any{
					"uuid": "jkl-012",
				},
				"status": map[string]any{
					"name": "",
				},
			},
			wantNil: true,
		},
		{
			name:    "not a map",
			entity:  "invalid",
			wantNil: true,
		},
		{
			name:    "nil",
			entity:  nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseVMv3Entity(tt.entity)
			if tt.wantNil {
				if got != nil {
					t.Errorf("parseVMv3Entity() = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("parseVMv3Entity() = nil, want non-nil")
			}
			if *got != tt.want {
				t.Errorf("parseVMv3Entity() = %+v, want %+v", *got, tt.want)
			}
		})
	}
}
