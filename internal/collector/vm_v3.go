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
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ingka-group/nutanix-exporter/internal/nutanix"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	vmV3PageSize = 100
	vmV3MaxPages = 50
)

// VMv3Collector collects VM metadata from the Prism Central v3 API.
type VMv3Collector struct {
	pcAPI           nutanix.NutanixClient
	infoGauge       *prometheus.GaugeVec
	powerGauge      *prometheus.GaugeVec
	memorySizeGauge *prometheus.GaugeVec
	countGauge      *prometheus.GaugeVec
}

func NewVMv3Collector(pcAPI nutanix.NutanixClient) *VMv3Collector {
	return &VMv3Collector{
		pcAPI: pcAPI,
		infoGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "nutanix",
				Subsystem: "vm",
				Name:      "info",
				Help:      "VM metadata exposed as labels. Always 1.",
			},
			[]string{"cluster_name", "cluster_uuid", "vm_name", "vm_uuid", "power_state", "hypervisor_type"},
		),
		powerGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "nutanix",
				Subsystem: "vm",
				Name:      "power_state_info",
				Help:      "VM power state: 1 = ON, 0 = OFF.",
			},
			[]string{"cluster_name", "cluster_uuid", "vm_name"},
		),
		memorySizeGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "nutanix",
				Subsystem: "vm",
				Name:      "memory_size_bytes",
				Help:      "VM assigned memory in bytes.",
			},
			[]string{"cluster_name", "cluster_uuid", "vm_name"},
		),
		countGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "nutanix",
				Subsystem: "vm",
				Name:      "count",
				Help:      "Total number of VMs per cluster.",
			},
			[]string{"cluster_name", "cluster_uuid"},
		),
	}
}

func (c *VMv3Collector) Describe(ch chan<- *prometheus.Desc) {
	c.infoGauge.Describe(ch)
	c.powerGauge.Describe(ch)
	c.memorySizeGauge.Describe(ch)
	c.countGauge.Describe(ch)
}

func (c *VMv3Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	vms, err := c.fetchAllVMs(ctx)
	if err != nil {
		slog.Error("Error fetching VMs from v3 API", "error", err)
		return
	}

	c.infoGauge.Reset()
	c.powerGauge.Reset()
	c.memorySizeGauge.Reset()
	c.countGauge.Reset()

	type clusterKey struct {
		name string
		uuid string
	}
	clusterVMCount := make(map[clusterKey]float64)
	for _, vm := range vms {
		c.infoGauge.WithLabelValues(
			vm.clusterName,
			vm.clusterUUID,
			vm.name,
			vm.uuid,
			vm.powerState,
			vm.hypervisorType,
		).Set(1)

		powerValue := 0.0
		if strings.EqualFold(vm.powerState, "ON") {
			powerValue = 1.0
		}
		c.powerGauge.WithLabelValues(vm.clusterName, vm.clusterUUID, vm.name).Set(powerValue)

		if vm.memorySizeMib > 0 {
			c.memorySizeGauge.WithLabelValues(vm.clusterName, vm.clusterUUID, vm.name).Set(float64(vm.memorySizeMib) * 1048576)
		}

		clusterVMCount[clusterKey{vm.clusterName, vm.clusterUUID}]++
	}

	for ck, count := range clusterVMCount {
		c.countGauge.WithLabelValues(ck.name, ck.uuid).Set(count)
	}

	c.infoGauge.Collect(ch)
	c.powerGauge.Collect(ch)
	c.memorySizeGauge.Collect(ch)
	c.countGauge.Collect(ch)
}

type vmV3Info struct {
	name           string
	uuid           string
	clusterName    string
	clusterUUID    string
	powerState     string
	hypervisorType string
	memorySizeMib  int64
}

func (c *VMv3Collector) fetchAllVMs(ctx context.Context) ([]vmV3Info, error) {
	var allVMs []vmV3Info
	offset := 0

	for page := 0; page < vmV3MaxPages; page++ {
		slog.Info("Fetching VMs from v3 API", "page", page, "offset", offset)

		resp, err := c.pcAPI.MakeRequest(ctx, "POST", "/api/nutanix/v3/vms/list", nutanix.RequestOptions{
			Payload: map[string]any{
				"kind":   "vm",
				"length": vmV3PageSize,
				"offset": offset,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("v3 vms/list request failed (page %d): %w", page, err)
		}

		statusCode := resp.StatusCode
		var result map[string]any
		decodeErr := func() error {
			defer func() {
				if cerr := resp.Body.Close(); cerr != nil {
					slog.Error("Error closing response body", "error", cerr)
				}
			}()
			return json.NewDecoder(resp.Body).Decode(&result)
		}()
		if decodeErr != nil {
			return nil, fmt.Errorf("v3 vms/list decode failed (page %d, status %d): %w", page, statusCode, decodeErr)
		}

		if statusCode < 200 || statusCode >= 300 {
			slog.Error("Non-2xx response from v3 vms/list", "status", statusCode, "page", page)
			break
		}

		entities, ok := result["entities"].([]any)
		if !ok {
			slog.Warn("v3 vms/list response has no 'entities' array", "page", page)
			break
		}

		slog.Info("v3 vms/list response received", "page", page, "entities_count", len(entities))

		for _, entity := range entities {
			if vm := parseVMv3Entity(entity); vm != nil {
				allVMs = append(allVMs, *vm)
			}
		}

		if len(entities) < vmV3PageSize {
			break
		}
		offset += vmV3PageSize
	}

	slog.Info("v3 VM fetch complete", "total_vms", len(allVMs))
	return allVMs, nil
}

func parseVMv3Entity(entity any) *vmV3Info {
	ent, ok := entity.(map[string]any)
	if !ok {
		return nil
	}

	metadata, _ := ent["metadata"].(map[string]any)
	status, _ := ent["status"].(map[string]any)
	if status == nil {
		return nil
	}

	uuid, _ := metadata["uuid"].(string)
	name, _ := status["name"].(string)
	if name == "" {
		return nil
	}

	clusterName := ""
	clusterUUID := ""
	if clusterRef, ok := status["cluster_reference"].(map[string]any); ok {
		clusterName, _ = clusterRef["name"].(string)
		clusterUUID, _ = clusterRef["uuid"].(string)
	}

	powerState := ""
	hypervisorType := ""
	var memorySizeMib int64
	if resources, ok := status["resources"].(map[string]any); ok {
		powerState, _ = resources["power_state"].(string)
		hypervisorType, _ = resources["hypervisor_type"].(string)
		if mib, ok := resources["memory_size_mib"].(float64); ok {
			memorySizeMib = int64(mib)
		}
	}

	return &vmV3Info{
		name:           name,
		uuid:           uuid,
		clusterName:    clusterName,
		clusterUUID:    clusterUUID,
		powerState:     powerState,
		hypervisorType: hypervisorType,
		memorySizeMib:  memorySizeMib,
	}
}
