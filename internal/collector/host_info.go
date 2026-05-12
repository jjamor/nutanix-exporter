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
	"strings"
	"time"

	"github.com/ingka-group/nutanix-exporter/internal/nutanix"
	"github.com/prometheus/client_golang/prometheus"
)

// HostInfoCollector exposes host metadata and state from the PE v2 API.
type HostInfoCollector struct {
	clusterName string
	api         nutanix.NutanixClient
	infoGauge   *prometheus.GaugeVec
	stateGauge  *prometheus.GaugeVec
}

func NewHostInfoCollector(clusterName string, api nutanix.NutanixClient) *HostInfoCollector {
	return &HostInfoCollector{
		clusterName: clusterName,
		api:         api,
		infoGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "nutanix",
				Subsystem: "host",
				Name:      "info",
				Help:      "Host metadata exposed as labels. Always 1.",
			},
			[]string{"cluster_name", "host_name", "host_uuid", "serial", "block_model_name", "cpu_model", "hypervisor_type", "hypervisor_full_name"},
		),
		stateGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "nutanix",
				Subsystem: "host",
				Name:      "state",
				Help:      "Host state: 1 = NORMAL, 0 = other.",
			},
			[]string{"cluster_name", "host_name"},
		),
	}
}

func (c *HostInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	c.infoGauge.Describe(ch)
	c.stateGauge.Describe(ch)
}

func (c *HostInfoCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.api.MakeRequest(ctx, "GET", "/v2.0/hosts/")
	if err != nil {
		slog.Error("Error fetching hosts for info collector", "cluster", c.clusterName, "error", err)
		return
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			slog.Error("Error closing response body", "cluster", c.clusterName, "error", cerr)
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Error("Non-2xx response from hosts API", "cluster", c.clusterName, "status", resp.Status)
		return
	}

	var result map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		slog.Error("Error decoding hosts response", "cluster", c.clusterName, "error", err)
		return
	}

	entities, ok := result["entities"].([]any)
	if !ok {
		return
	}

	c.infoGauge.Reset()
	c.stateGauge.Reset()

	for _, entity := range entities {
		ent, ok := entity.(map[string]any)
		if !ok {
			continue
		}

		name, _ := ent["name"].(string)
		if name == "" {
			continue
		}

		uuid, _ := ent["uuid"].(string)
		serial, _ := ent["serial"].(string)
		blockModelName, _ := ent["block_model_name"].(string)
		cpuModel, _ := ent["cpu_model"].(string)
		hypervisorType, _ := ent["hypervisor_type"].(string)
		hypervisorFullName, _ := ent["hypervisor_full_name"].(string)
		state, _ := ent["state"].(string)

		c.infoGauge.WithLabelValues(
			c.clusterName,
			name,
			uuid,
			serial,
			blockModelName,
			cpuModel,
			hypervisorType,
			hypervisorFullName,
		).Set(1)

		stateValue := 0.0
		if strings.EqualFold(state, "NORMAL") {
			stateValue = 1.0
		}
		c.stateGauge.WithLabelValues(c.clusterName, name).Set(stateValue)
	}

	c.infoGauge.Collect(ch)
	c.stateGauge.Collect(ch)
}
