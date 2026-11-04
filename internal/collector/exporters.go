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

import (
	"github.com/ingka-group/nutanix-exporter/internal/nutanix"
)

// The generic collectors label every series with cluster_name (the cluster that
// was queried) and pe_cluster_name (the cluster the entity really belongs to).

func NewClusterCollector(clusterName string, api nutanix.NutanixClient, configPath string, resolver *ClusterResolver) (*Exporter, error) {
	labels := []string{"cluster_name", PEClusterLabel}
	e := NewExporter(clusterName, api, "/v2.0/cluster/", labels)
	e.resolver = resolver
	if err := e.initMetricsFromFile(configPath, labels); err != nil {
		return nil, err
	}
	return e, nil
}

func NewHostCollector(clusterName string, api nutanix.NutanixClient, configPath string, resolver *ClusterResolver) (*Exporter, error) {
	labels := []string{"cluster_name", "host_name", PEClusterLabel}
	e := NewExporter(clusterName, api, "/v2.0/hosts/", labels)
	e.resolver = resolver
	if err := e.initMetricsFromFile(configPath, labels); err != nil {
		return nil, err
	}
	return e, nil
}

func NewVMCollector(clusterName string, api nutanix.NutanixClient, configPath string, resolver *ClusterResolver) (*Exporter, error) {
	labels := []string{"cluster_name", "vm_name", PEClusterLabel}
	e := NewExporter(clusterName, api, "/v2.0/vms/", labels)
	e.resolver = resolver
	if err := e.initMetricsFromFile(configPath, labels); err != nil {
		return nil, err
	}
	return e, nil
}

func NewVMv1Collector(clusterName string, api nutanix.NutanixClient, configPath string, resolver *ClusterResolver) (*Exporter, error) {
	labels := []string{"cluster_name", "vm_name", PEClusterLabel}
	e := NewExporter(clusterName, api, "/v1/vms/", labels)
	e.resolver = resolver
	if err := e.initMetricsFromFile(configPath, labels); err != nil {
		return nil, err
	}
	return e, nil
}

func NewStorageContainerCollector(clusterName string, api nutanix.NutanixClient, configPath string, resolver *ClusterResolver) (*Exporter, error) {
	labels := []string{"cluster_name", "container_name", PEClusterLabel}
	e := NewExporter(clusterName, api, "/v2.0/storage_containers/", labels)
	e.resolver = resolver
	if err := e.initMetricsFromFile(configPath, labels); err != nil {
		return nil, err
	}
	return e, nil
}
