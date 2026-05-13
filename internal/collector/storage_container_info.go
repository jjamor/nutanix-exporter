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

// StorageContainerInfoCollector exposes storage container metadata from the PE v2 API.
type StorageContainerInfoCollector struct {
	clusterName string
	api         nutanix.NutanixClient
	infoGauge   *prometheus.GaugeVec
}

func NewStorageContainerInfoCollector(clusterName string, api nutanix.NutanixClient) *StorageContainerInfoCollector {
	return &StorageContainerInfoCollector{
		clusterName: clusterName,
		api:         api,
		infoGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "nutanix",
				Subsystem: "storage_container",
				Name:      "info",
				Help:      "Storage container metadata exposed as labels. Always 1.",
			},
			[]string{"cluster_name", "container_name", "container_uuid", "replication_factor", "compression", "dedup", "erasure_code"},
		),
	}
}

func (c *StorageContainerInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	c.infoGauge.Describe(ch)
}

func (c *StorageContainerInfoCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.api.MakeRequest(ctx, "GET", "/v2.0/storage_containers/")
	if err != nil {
		slog.Error("Error fetching storage containers for info collector", "cluster", c.clusterName, "error", err)
		return
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			slog.Error("Error closing response body", "cluster", c.clusterName, "error", cerr)
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Error("Non-2xx response from storage containers API", "cluster", c.clusterName, "status", resp.Status)
		return
	}

	var result map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		slog.Error("Error decoding storage containers response", "cluster", c.clusterName, "error", err)
		return
	}

	entities, ok := result["entities"].([]any)
	if !ok {
		return
	}

	c.infoGauge.Reset()

	for _, entity := range entities {
		ent, ok := entity.(map[string]any)
		if !ok {
			continue
		}

		name, _ := ent["name"].(string)
		if name == "" {
			continue
		}

		uuid, _ := ent["storage_container_uuid"].(string)

		rf := ""
		if n, ok := ent["replication_factor"].(float64); ok {
			rf = fmt.Sprintf("%d", int64(n))
		}

		compression := "off"
		if v, ok := ent["compression_enabled"].(bool); ok && v {
			compression = "on"
		} else if s, ok := ent["compression_enabled"].(string); ok && strings.EqualFold(s, "on") {
			compression = "on"
		}

		dedup := "off"
		if v, ok := ent["on_disk_dedup"].(string); ok && strings.EqualFold(v, "on") {
			dedup = "on"
		}

		erasure := "off"
		if v, ok := ent["erasure_code"].(string); ok && strings.EqualFold(v, "on") {
			erasure = "on"
		}

		c.infoGauge.WithLabelValues(
			c.clusterName,
			name,
			uuid,
			rf,
			compression,
			dedup,
			erasure,
		).Set(1)
	}

	c.infoGauge.Collect(ch)
}
