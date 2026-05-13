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

// ClusterStatsCollector computes cluster-level capacity and usage metrics
// by aggregating data from all hosts via /v2.0/hosts/.
type ClusterStatsCollector struct {
	clusterName          string
	api                  nutanix.NutanixClient
	cpuCapacity          prometheus.Gauge
	cpuUsageHz           prometheus.Gauge
	cpuUsagePercent      prometheus.Gauge
	memoryCapacity       prometheus.Gauge
	memoryUsageBytes     prometheus.Gauge
	memoryUsagePercent   prometheus.Gauge
	storageCapacity      prometheus.Gauge
	storageUsed          prometheus.Gauge
	storageFree          prometheus.Gauge
	iops                 prometheus.Gauge
	readBytesPerSec      prometheus.Gauge
	writeBytesPerSec     prometheus.Gauge
}

func NewClusterStatsCollector(clusterName string, api nutanix.NutanixClient) *ClusterStatsCollector {
	labels := prometheus.Labels{"cluster_name": clusterName}
	return &ClusterStatsCollector{
		clusterName: clusterName,
		api:         api,
		cpuCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "nutanix",
			Subsystem:   "cluster",
			Name:        "cpu_capacity_hz",
			Help:        "Total CPU capacity of the cluster in Hz (sum of all hosts).",
			ConstLabels: labels,
		}),
		cpuUsageHz: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "nutanix",
			Subsystem:   "cluster",
			Name:        "cpu_usage_hz",
			Help:        "Current CPU usage of the cluster in Hz.",
			ConstLabels: labels,
		}),
		cpuUsagePercent: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "nutanix",
			Subsystem:   "cluster",
			Name:        "cpu_usage_percent",
			Help:        "Current CPU usage of the cluster as a percentage.",
			ConstLabels: labels,
		}),
		memoryCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "nutanix",
			Subsystem:   "cluster",
			Name:        "memory_capacity_bytes",
			Help:        "Total memory capacity of the cluster in bytes (sum of all hosts).",
			ConstLabels: labels,
		}),
		memoryUsageBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "nutanix",
			Subsystem:   "cluster",
			Name:        "memory_usage_bytes",
			Help:        "Current memory usage of the cluster in bytes.",
			ConstLabels: labels,
		}),
		memoryUsagePercent: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "nutanix",
			Subsystem:   "cluster",
			Name:        "memory_usage_percent",
			Help:        "Current memory usage of the cluster as a percentage.",
			ConstLabels: labels,
		}),
		storageCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "nutanix",
			Subsystem:   "cluster",
			Name:        "storage_capacity_bytes",
			Help:        "Total storage capacity of the cluster in bytes.",
			ConstLabels: labels,
		}),
		storageUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "nutanix",
			Subsystem:   "cluster",
			Name:        "storage_used_bytes",
			Help:        "Total storage used in the cluster in bytes.",
			ConstLabels: labels,
		}),
		storageFree: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "nutanix",
			Subsystem:   "cluster",
			Name:        "storage_free_bytes",
			Help:        "Total free storage in the cluster in bytes.",
			ConstLabels: labels,
		}),
		iops: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "nutanix",
			Subsystem:   "cluster",
			Name:        "iops",
			Help:        "Total IOPS across the cluster (sum of all hosts).",
			ConstLabels: labels,
		}),
		readBytesPerSec: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "nutanix",
			Subsystem:   "cluster",
			Name:        "read_bytes_per_sec",
			Help:        "Total read throughput across the cluster in bytes/s.",
			ConstLabels: labels,
		}),
		writeBytesPerSec: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "nutanix",
			Subsystem:   "cluster",
			Name:        "write_bytes_per_sec",
			Help:        "Total write throughput across the cluster in bytes/s.",
			ConstLabels: labels,
		}),
	}
}

func (c *ClusterStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	c.cpuCapacity.Describe(ch)
	c.cpuUsageHz.Describe(ch)
	c.cpuUsagePercent.Describe(ch)
	c.memoryCapacity.Describe(ch)
	c.memoryUsageBytes.Describe(ch)
	c.memoryUsagePercent.Describe(ch)
	c.storageCapacity.Describe(ch)
	c.storageUsed.Describe(ch)
	c.storageFree.Describe(ch)
	c.iops.Describe(ch)
	c.readBytesPerSec.Describe(ch)
	c.writeBytesPerSec.Describe(ch)
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

	var totalCPUCapacity, totalMemCapacity float64
	var totalCPUUsage, totalMemUsage float64
	var maxStorageCapacity, totalStorageUsed float64
	var totalIOPS, totalReadKbps, totalWriteKbps float64

	for _, entity := range entities {
		ent, ok := entity.(map[string]any)
		if !ok {
			continue
		}

		cpuCap, _ := ent["cpu_capacity_in_hz"].(float64)
		memCap, _ := ent["memory_capacity_in_bytes"].(float64)

		totalCPUCapacity += cpuCap
		totalMemCapacity += memCap

		if stats, ok := ent["stats"].(map[string]any); ok {
			if ppmStr, ok := stats["hypervisor_cpu_usage_ppm"].(string); ok {
				if ppm, err := strconv.ParseFloat(ppmStr, 64); err == nil {
					totalCPUUsage += cpuCap * ppm / 1000000
				}
			}
			if ppmStr, ok := stats["hypervisor_memory_usage_ppm"].(string); ok {
				if ppm, err := strconv.ParseFloat(ppmStr, 64); err == nil {
					totalMemUsage += memCap * ppm / 1000000
				}
			}
			if v, ok := stats["num_iops"].(string); ok {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					totalIOPS += f
				}
			}
			if v, ok := stats["read_io_bandwidth_kBps"].(string); ok {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					totalReadKbps += f
				}
			}
			if v, ok := stats["write_io_bandwidth_kBps"].(string); ok {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					totalWriteKbps += f
				}
			}
		}

		if usageStats, ok := ent["usage_stats"].(map[string]any); ok {
			if v, ok := usageStats["storage.capacity_bytes"].(string); ok {
				if f, err := strconv.ParseFloat(v, 64); err == nil && f > maxStorageCapacity {
					maxStorageCapacity = f
				}
			}
			if v, ok := usageStats["storage.usage_bytes"].(string); ok {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					totalStorageUsed += f
				}
			}
		}
	}

	c.cpuCapacity.Set(totalCPUCapacity)
	c.cpuUsageHz.Set(totalCPUUsage)
	if totalCPUCapacity > 0 {
		c.cpuUsagePercent.Set(totalCPUUsage / totalCPUCapacity * 100)
	}

	c.memoryCapacity.Set(totalMemCapacity)
	c.memoryUsageBytes.Set(totalMemUsage)
	if totalMemCapacity > 0 {
		c.memoryUsagePercent.Set(totalMemUsage / totalMemCapacity * 100)
	}

	c.storageCapacity.Set(maxStorageCapacity)
	c.storageUsed.Set(totalStorageUsed)
	if maxStorageCapacity > totalStorageUsed {
		c.storageFree.Set(maxStorageCapacity - totalStorageUsed)
	} else {
		c.storageFree.Set(0)
	}

	c.iops.Set(totalIOPS)
	c.readBytesPerSec.Set(totalReadKbps * 1024)
	c.writeBytesPerSec.Set(totalWriteKbps * 1024)

	c.cpuCapacity.Collect(ch)
	c.cpuUsageHz.Collect(ch)
	c.cpuUsagePercent.Collect(ch)
	c.memoryCapacity.Collect(ch)
	c.memoryUsageBytes.Collect(ch)
	c.memoryUsagePercent.Collect(ch)
	c.storageCapacity.Collect(ch)
	c.storageUsed.Collect(ch)
	c.storageFree.Collect(ch)
	c.iops.Collect(ch)
	c.readBytesPerSec.Collect(ch)
	c.writeBytesPerSec.Collect(ch)
}
