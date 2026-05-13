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
	"time"

	"github.com/ingka-group/nutanix-exporter/internal/nutanix"
	"github.com/prometheus/client_golang/prometheus"
)

// ClusterInfoCollector exposes cluster metadata from the PE v2 API.
type ClusterInfoCollector struct {
	clusterName string
	api         nutanix.NutanixClient
	infoGauge   *prometheus.GaugeVec
}

func NewClusterInfoCollector(clusterName string, api nutanix.NutanixClient) *ClusterInfoCollector {
	return &ClusterInfoCollector{
		clusterName: clusterName,
		api:         api,
		infoGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "nutanix",
				Subsystem: "cluster",
				Name:      "info",
				Help:      "Cluster metadata exposed as labels. Always 1.",
			},
			[]string{"cluster_name", "cluster_uuid", "version", "timezone", "num_nodes", "external_ip"},
		),
	}
}

func (c *ClusterInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	c.infoGauge.Describe(ch)
}

func (c *ClusterInfoCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.api.MakeRequest(ctx, "GET", "/v2.0/cluster/")
	if err != nil {
		slog.Error("Error fetching cluster info", "cluster", c.clusterName, "error", err)
		return
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			slog.Error("Error closing response body", "cluster", c.clusterName, "error", cerr)
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Error("Non-2xx response from cluster API", "cluster", c.clusterName, "status", resp.Status)
		return
	}

	var result map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		slog.Error("Error decoding cluster response", "cluster", c.clusterName, "error", err)
		return
	}

	c.infoGauge.Reset()

	uuid, _ := result["uuid"].(string)
	version, _ := result["version"].(string)
	timezone, _ := result["timezone"].(string)
	externalIP, _ := result["cluster_external_ipaddress"].(string)

	numNodes := ""
	if n, ok := result["num_nodes"].(float64); ok {
		numNodes = fmt.Sprintf("%d", int64(n))
	}

	c.infoGauge.WithLabelValues(
		c.clusterName,
		uuid,
		version,
		timezone,
		numNodes,
		externalIP,
	).Set(1)

	c.infoGauge.Collect(ch)
}
