package collector

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ingka-group/nutanix-exporter/internal/nutanix"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// fakeAPI answers every request with the JSON registered for its path.
type fakeAPI struct {
	responses map[string]string
}

func (f *fakeAPI) MakeRequest(_ context.Context, _, action string, _ ...nutanix.RequestOptions) (*http.Response, error) {
	body, ok := f.responses[action]
	if !ok {
		return &http.Response{StatusCode: http.StatusNotFound, Status: "404", Body: io.NopCloser(strings.NewReader("{}"))}, nil
	}
	return &http.Response{StatusCode: http.StatusOK, Status: "200", Body: io.NopCloser(strings.NewReader(body))}, nil
}

const (
	uuidRL01 = "uuid-rl01"
	uuidRL02 = "uuid-rl02"
	pcName   = "NTX-PrismCentral-RL01"
)

// hostsJSON mimics /v2.0/hosts/ queried on a Prism Central: hosts of two clusters.
const hostsJSON = `{"entities":[
 {"name":"HO01","uuid":"h1","cluster_uuid":"` + uuidRL01 + `","state":"NORMAL","num_vms":3,
  "cpu_capacity_in_hz":10000,"memory_capacity_in_bytes":1000,
  "stats":{"hypervisor_cpu_usage_ppm":"500000","num_iops":"10"},
  "usage_stats":{"storage.capacity_bytes":"1000","storage.usage_bytes":"100"}},
 {"name":"HO02","uuid":"h2","cluster_uuid":"` + uuidRL01 + `","state":"NORMAL","num_vms":2,
  "cpu_capacity_in_hz":10000,"memory_capacity_in_bytes":1000,
  "stats":{"hypervisor_cpu_usage_ppm":"500000","num_iops":"20"},
  "usage_stats":{"storage.capacity_bytes":"1000","storage.usage_bytes":"200"}},
 {"name":"HO21","uuid":"h3","cluster_uuid":"` + uuidRL02 + `","state":"NORMAL","num_vms":1,
  "cpu_capacity_in_hz":20000,"memory_capacity_in_bytes":2000,
  "stats":{"hypervisor_cpu_usage_ppm":"250000","num_iops":"5"},
  "usage_stats":{"storage.capacity_bytes":"500","storage.usage_bytes":"50"}}
]}`

func newTestResolver() *ClusterResolver {
	r := NewClusterResolver()
	r.AddClusters(map[string]string{uuidRL01: "NTX-RL01", uuidRL02: "NTX-RL02"})
	return r
}

// gatherLabels returns, for every sample of the named metric, its labels as a map.
func gatherLabels(t *testing.T, c prometheus.Collector, metric string) []map[string]string {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	var out []map[string]string
	for _, mf := range families {
		if mf.GetName() != metric {
			continue
		}
		for _, m := range mf.GetMetric() {
			out = append(out, labelMap(m))
		}
	}
	return out
}

func labelMap(m *dto.Metric) map[string]string {
	labels := map[string]string{}
	for _, lp := range m.GetLabel() {
		labels[lp.GetName()] = lp.GetValue()
	}
	return labels
}

func Test_ClusterResolver(t *testing.T) {
	r := newTestResolver()
	r.SetVMs(map[string]string{"vm-1": "NTX-RL02"})

	tests := []struct {
		name string
		ent  map[string]any
		want string
	}{
		{"cluster_uuid known", map[string]any{"cluster_uuid": uuidRL02}, "NTX-RL02"},
		{"cluster_uuid unknown falls back", map[string]any{"cluster_uuid": "nope"}, pcName},
		{"VM resolved by its uuid", map[string]any{"uuid": "vm-1"}, "NTX-RL02"},
		{"no identifiers falls back", map[string]any{"name": "x"}, pcName},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := r.ForEntity(tt.ent, pcName); got != tt.want {
				t.Errorf("ForEntity() = %q, want %q", got, tt.want)
			}
		})
	}

	var nilResolver *ClusterResolver
	if got := nilResolver.ForEntity(map[string]any{"cluster_uuid": uuidRL01}, pcName); got != pcName {
		t.Errorf("nil resolver ForEntity() = %q, want fallback %q", got, pcName)
	}
}

func Test_ClusterResolver_AddClustersKeepsOldEntries(t *testing.T) {
	r := newTestResolver()
	r.AddClusters(map[string]string{"other": "NTX-X", "": "ignored"})
	if got := r.Name(uuidRL01, "fb"); got != "NTX-RL01" {
		t.Errorf("Name() = %q, want NTX-RL01 (entry lost after merge)", got)
	}
	if got := r.Name("other", "fb"); got != "NTX-X" {
		t.Errorf("Name() = %q, want NTX-X", got)
	}
}

func Test_HostInfoCollector_labels(t *testing.T) {
	api := &fakeAPI{responses: map[string]string{"/v2.0/hosts/": hostsJSON}}
	c := NewHostInfoCollector(pcName, api, newTestResolver())

	want := map[string]string{"HO01": "NTX-RL01", "HO02": "NTX-RL01", "HO21": "NTX-RL02"}
	samples := gatherLabels(t, c, "nutanix_host_info")
	if len(samples) != len(want) {
		t.Fatalf("got %d samples, want %d", len(samples), len(want))
	}
	for _, l := range samples {
		if l["cluster_name"] != pcName {
			t.Errorf("host %s: cluster_name = %q, want %q (must not change)", l["host_name"], l["cluster_name"], pcName)
		}
		if l[PEClusterLabel] != want[l["host_name"]] {
			t.Errorf("host %s: %s = %q, want %q", l["host_name"], PEClusterLabel, l[PEClusterLabel], want[l["host_name"]])
		}
	}
}

func Test_HostInfoCollector_noResolverKeepsFallback(t *testing.T) {
	api := &fakeAPI{responses: map[string]string{"/v2.0/hosts/": hostsJSON}}
	c := NewHostInfoCollector(pcName, api, nil)
	for _, l := range gatherLabels(t, c, "nutanix_host_info") {
		if l[PEClusterLabel] != pcName {
			t.Errorf("host %s: %s = %q, want fallback %q", l["host_name"], PEClusterLabel, l[PEClusterLabel], pcName)
		}
	}
}

func Test_StorageContainerInfoCollector_labels(t *testing.T) {
	api := &fakeAPI{responses: map[string]string{"/v2.0/storage_containers/": `{"entities":[
	 {"name":"C1","storage_container_uuid":"c1","cluster_uuid":"` + uuidRL02 + `","replication_factor":2,
	  "usage_stats":{"storage.capacity_bytes":"100","storage.usage_bytes":"10"}}]}`}}
	c := NewStorageContainerInfoCollector(pcName, api, newTestResolver())

	for _, metric := range []string{"nutanix_storage_container_info", "nutanix_storage_container_usage_percent"} {
		samples := gatherLabels(t, c, metric)
		if len(samples) != 1 {
			t.Fatalf("%s: got %d samples, want 1", metric, len(samples))
		}
		if samples[0]["cluster_name"] != pcName || samples[0][PEClusterLabel] != "NTX-RL02" {
			t.Errorf("%s: labels = %v", metric, samples[0])
		}
	}
}

func Test_GenericCollectors_labels(t *testing.T) {
	api := &fakeAPI{responses: map[string]string{
		"/v2.0/hosts/": hostsJSON,
		"/v1/vms/": `{"entities":[
		 {"vmName":"vm-a","uuid":"vm-1","numVCpus":4}]}`,
	}}
	resolver := newTestResolver()
	resolver.SetVMs(map[string]string{"vm-1": "NTX-RL02"})

	dir := t.TempDir()
	hostCfg := writeFile(t, dir, "host.yaml", "- name: num_vms\n  help: VMs.\n")
	vmCfg := writeFile(t, dir, "vm_v1.yaml", "- name: numvcpus\n  help: vCPUs.\n")

	hostCol, err := NewHostCollector(pcName, api, hostCfg, resolver)
	if err != nil {
		t.Fatal(err)
	}
	hostWant := map[string]string{"HO01": "NTX-RL01", "HO02": "NTX-RL01", "HO21": "NTX-RL02"}
	samples := gatherLabels(t, hostCol, "nutanix_host_num_vms")
	if len(samples) != len(hostWant) {
		t.Fatalf("host: got %d samples, want %d", len(samples), len(hostWant))
	}
	for _, l := range samples {
		if l["cluster_name"] != pcName || l[PEClusterLabel] != hostWant[l["host_name"]] {
			t.Errorf("host labels = %v", l)
		}
	}

	vmCol, err := NewVMv1Collector(pcName, api, vmCfg, resolver)
	if err != nil {
		t.Fatal(err)
	}
	samples = gatherLabels(t, vmCol, "nutanix_vm_v1_numvcpus")
	if len(samples) != 1 || samples[0]["vm_name"] != "vm-a" || samples[0]["cluster_name"] != pcName || samples[0][PEClusterLabel] != "NTX-RL02" {
		t.Errorf("vm v1 labels = %v", samples)
	}
}

func Test_ClusterStatsCollector_notSplit(t *testing.T) {
	api := &fakeAPI{responses: map[string]string{"/v2.0/hosts/": hostsJSON}}
	c := NewClusterStatsCollector(pcName, api, newTestResolver(), false)

	samples := gatherLabels(t, c, "nutanix_cluster_iops")
	if len(samples) != 1 {
		t.Fatalf("got %d series, want 1 (sum of everything, as before)", len(samples))
	}
	if samples[0]["cluster_name"] != pcName || samples[0][PEClusterLabel] != pcName {
		t.Errorf("labels = %v", samples[0])
	}
	if got := gaugeValue(t, c, "nutanix_cluster_iops", nil); got != 35 {
		t.Errorf("iops = %v, want 35", got)
	}
}

func Test_ClusterStatsCollector_split(t *testing.T) {
	api := &fakeAPI{responses: map[string]string{"/v2.0/hosts/": hostsJSON}}
	c := NewClusterStatsCollector(pcName, api, newTestResolver(), true)

	if n := len(gatherLabels(t, c, "nutanix_cluster_iops")); n != 2 {
		t.Fatalf("got %d series, want 2 (one per real cluster)", n)
	}
	rl01 := map[string]string{PEClusterLabel: "NTX-RL01"}
	rl02 := map[string]string{PEClusterLabel: "NTX-RL02"}

	checks := []struct {
		metric string
		sel    map[string]string
		want   float64
	}{
		{"nutanix_cluster_iops", rl01, 30},
		{"nutanix_cluster_iops", rl02, 5},
		{"nutanix_cluster_cpu_capacity_hz", rl01, 20000},
		{"nutanix_cluster_cpu_usage_percent", rl01, 50},
		{"nutanix_cluster_cpu_usage_percent", rl02, 25},
		{"nutanix_cluster_storage_used_bytes", rl01, 300},
		{"nutanix_cluster_storage_free_bytes", rl02, 450},
	}
	for _, ck := range checks {
		if got := gaugeValue(t, c, ck.metric, ck.sel); got != ck.want {
			t.Errorf("%s %v = %v, want %v", ck.metric, ck.sel, got, ck.want)
		}
	}
}

func gaugeValue(t *testing.T, c prometheus.Collector, metric string, sel map[string]string) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != metric {
			continue
		}
	next:
		for _, m := range mf.GetMetric() {
			labels := labelMap(m)
			for k, v := range sel {
				if labels[k] != v {
					continue next
				}
			}
			return m.GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s %v not found", metric, sel)
	return 0
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
