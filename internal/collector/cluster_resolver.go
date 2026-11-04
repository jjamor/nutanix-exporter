/*
Copyright © 2024-2026 Ingka Holding B.V. All Rights Reserved.
Licensed under the GPL, Version 3 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

       <https://www.gnu.org/licenses/gpl-3.0.en.html>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package collector

import "sync"

// PEClusterLabel is the label that carries the name of the Prism Element cluster
// an entity really belongs to. It is separate from cluster_name because, when the
// exporter queries a Prism Central, cluster_name is the Prism Central name for
// every host, container and v1 VM, whatever cluster they live in.
const PEClusterLabel = "pe_cluster_name"

// ClusterResolver translates the identifiers returned by the Nutanix APIs into
// the name of the cluster that owns the entity. It is shared by all collectors
// and is safe for concurrent use. A nil *ClusterResolver is valid and always
// returns the fallback.
type ClusterResolver struct {
	mu       sync.RWMutex
	clusters map[string]string // cluster UUID -> cluster name
	vms      map[string]string // VM UUID -> cluster name
}

func NewClusterResolver() *ClusterResolver {
	return &ClusterResolver{
		clusters: make(map[string]string),
		vms:      make(map[string]string),
	}
}

// AddClusters merges entries into the cluster UUID -> name table. Entries are
// only added or updated, never removed: a stale name is harmless, a missing one
// would make the label fall back to the Prism Central name.
func (r *ClusterResolver) AddClusters(clusters map[string]string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	for uuid, name := range clusters {
		if uuid != "" && name != "" {
			r.clusters[uuid] = name
		}
	}
	r.mu.Unlock()
}

// SetVMs replaces the VM UUID -> cluster name table. It is fed by the Prism
// Central v3 VM collector, the only source that knows the cluster of every VM.
func (r *ClusterResolver) SetVMs(vms map[string]string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.vms = vms
	r.mu.Unlock()
}

// Name returns the name of the cluster with the given UUID, or fallback if unknown.
func (r *ClusterResolver) Name(clusterUUID, fallback string) string {
	if r == nil || clusterUUID == "" {
		return fallback
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if name, ok := r.clusters[clusterUUID]; ok && name != "" {
		return name
	}
	return fallback
}

// ForEntity returns the cluster an API entity belongs to, or fallback if it
// cannot be determined. It uses the cluster UUID the entity carries (v2 hosts
// and storage containers) and, if it has none, the entity UUID looked up among
// the VMs known from Prism Central (v1 VMs).
func (r *ClusterResolver) ForEntity(ent map[string]any, fallback string) string {
	if r == nil {
		return fallback
	}
	for _, key := range []string{"cluster_uuid", "clusterUuid"} {
		if uuid, ok := ent[key].(string); ok && uuid != "" {
			return r.Name(uuid, fallback)
		}
	}
	if uuid, ok := ent["uuid"].(string); ok && uuid != "" {
		r.mu.RLock()
		defer r.mu.RUnlock()
		if name, ok := r.vms[uuid]; ok && name != "" {
			return name
		}
	}
	return fallback
}
