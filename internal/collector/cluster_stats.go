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
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/ingka-group/nutanix-exporter/internal/nutanix"
	"github.com/prometheus/client_golang/prometheus"
)

// clusterStatsMetrics lists the cluster-level metrics derived from hosts.
var clusterStatsMetrics = []struct{ name, help string }{
	{"cpu_capacity_hz", "Total CPU capacity of the cluster in Hz (sum of all hosts)."},
	{"cpu_usage_hz", "Current CPU usage of the cluster in Hz."},
	{"cpu_usage_percent", "Current CPU usage of the cluster as a percentage."},
	{"memory_capacity_bytes", "Total memory capacity of the cluster in bytes (sum of all hosts)."},
	{"memory_usage_bytes", "Current memory usage of the cluster in bytes."},
	{"memory_usage_percent", "Current memory usage of the cluster as a percentage."},
	{"storage_capacity_bytes", "Total storage capacity of the cluster in bytes."},
	{"storage_used_bytes", "Total storage used in the cluster in bytes."},
	{"storage_free_bytes", "Total free storage in the cluster in bytes."},
	{"iops", "Total IOPS across the cluster (sum of all hosts)."},
	{"read_bytes_per_sec", "Total read throughput across the cluster in bytes/s."},
	{"write_bytes_per_sec", "Total write throughput across the cluster in bytes/s."},
}

// clusterAccumulator sums the hosts that belong to one cluster.
type clusterAccumulator struct {
	cpuCapacity, memCapacity        float64
	cpuUsage, memUsage              float64
	maxStorageCapacity, storageUsed float64
	iops, readKbps, writeKbps       float64
}

// ClusterStatsCollector computes cluster-level capacity and usage metrics
// by aggregating data from all hosts via /v2.0/hosts/.
//
// When the exporter queries a Prism Central, that endpoint returns the hosts of
// every cluster it manages. By default they are all summed into one series
// (pe_cluster_name = the queried cluster, as cluster_name). With splitByCluster
// they are grouped by the cluster each host belongs to, giving one series per
// real cluster.
type ClusterStatsCollector struct {
	clusterName    string
	api            nutanix.NutanixClient
	resolver       *ClusterResolver
	splitByCluster bool
	gauges         map[string]*prometheus.GaugeVec
}

func NewClusterStatsCollector(clusterName string, api nutanix.NutanixClient, resolver *ClusterResolver, splitByCluster bool) *ClusterStatsCollector {
	gauges := make(map[string]*prometheus.GaugeVec, len(clusterStatsMetrics))
	for _, m := range clusterStatsMetrics {
		gauges[m.name] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "nutanix",
			Subsystem: "cluster",
			Name:      m.name,
			Help:      m.help,
		}, []string{"cluster_name", PEClusterLabel})
	}
	return &ClusterStatsCollector{
		clusterName:    clusterName,
		api:            api,
		resolver:       resolver,
		splitByCluster: splitByCluster,
		gauges:         gauges,
	}
}

func (c *ClusterStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, g := range c.gauges {
		g.Describe(ch)
	}
}

func (c *ClusterStatsCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.api.MakeRequest(ctx, "GET", "/v2.0/hosts/")
	if err != nil {
		slog.Error("Error fetching hosts for cluster stats", "cluster", c.clusterName, "error", err)
		return
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			slog.Error("Error closing response body", "cluster", c.clusterName, "error", cerr)
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Error("Non-2xx response from hosts API for cluster stats", "cluster", c.clusterName, "status", resp.Status)
		return
	}

	var result map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		slog.Error("Error decoding hosts response for cluster stats", "cluster", c.clusterName, "error", err)
		return
	}

	entities, ok := result["entities"].([]any)
	if !ok || len(entities) == 0 {
		return
	}

	groups := make(map[string]*clusterAccumulator)
	for _, entity := range entities {
		ent, ok := entity.(map[string]any)
		if !ok {
			continue
		}

		peCluster := c.clusterName
		if c.splitByCluster {
			peCluster = c.resolver.ForEntity(ent, c.clusterName)
		}
		acc := groups[peCluster]
		if acc == nil {
			acc = &clusterAccumulator{}
			groups[peCluster] = acc
		}
		acc.addHost(ent)
	}

	for _, g := range c.gauges {
		g.Reset()
	}
	for peCluster, acc := range groups {
		c.publish(peCluster, acc)
	}
	for _, g := range c.gauges {
		g.Collect(ch)
	}
}

// addHost adds the capacity and usage of one /v2.0/hosts/ entity.
func (a *clusterAccumulator) addHost(ent map[string]any) {
	cpuCap, _ := ent["cpu_capacity_in_hz"].(float64)
	memCap, _ := ent["memory_capacity_in_bytes"].(float64)

	a.cpuCapacity += cpuCap
	a.memCapacity += memCap

	if stats, ok := ent["stats"].(map[string]any); ok {
		if ppm, ok := statFloat(stats, "hypervisor_cpu_usage_ppm"); ok {
			a.cpuUsage += cpuCap * ppm / 1000000
		}
		if ppm, ok := statFloat(stats, "hypervisor_memory_usage_ppm"); ok {
			a.memUsage += memCap * ppm / 1000000
		}
		if f, ok := statFloat(stats, "num_iops"); ok {
			a.iops += f
		}
		if f, ok := statFloat(stats, "read_io_bandwidth_kBps"); ok {
			a.readKbps += f
		}
		if f, ok := statFloat(stats, "write_io_bandwidth_kBps"); ok {
			a.writeKbps += f
		}
	}

	if usageStats, ok := ent["usage_stats"].(map[string]any); ok {
		if f, ok := statFloat(usageStats, "storage.capacity_bytes"); ok && f > a.maxStorageCapacity {
			a.maxStorageCapacity = f
		}
		if f, ok := statFloat(usageStats, "storage.usage_bytes"); ok {
			a.storageUsed += f
		}
	}
}

// statFloat reads a numeric value that the API returns as a string.
func statFloat(m map[string]any, key string) (float64, bool) {
	str, ok := m[key].(string)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(str, 64)
	return f, err == nil
}

func (c *ClusterStatsCollector) publish(peCluster string, a *clusterAccumulator) {
	set := func(name string, v float64) {
		c.gauges[name].WithLabelValues(c.clusterName, peCluster).Set(v)
	}

	set("cpu_capacity_hz", a.cpuCapacity)
	set("cpu_usage_hz", a.cpuUsage)
	if a.cpuCapacity > 0 {
		set("cpu_usage_percent", a.cpuUsage/a.cpuCapacity*100)
	}

	set("memory_capacity_bytes", a.memCapacity)
	set("memory_usage_bytes", a.memUsage)
	if a.memCapacity > 0 {
		set("memory_usage_percent", a.memUsage/a.memCapacity*100)
	}

	set("storage_capacity_bytes", a.maxStorageCapacity)
	set("storage_used_bytes", a.storageUsed)
	if a.maxStorageCapacity > a.storageUsed {
		set("storage_free_bytes", a.maxStorageCapacity-a.storageUsed)
	} else {
		set("storage_free_bytes", 0)
	}

	set("iops", a.iops)
	set("read_bytes_per_sec", a.readKbps*1024)
	set("write_bytes_per_sec", a.writeKbps*1024)
}
