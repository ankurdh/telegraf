package vsphere

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vmware/govmomi/vim25/soap"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs/vsphere/vsan/methods"
	vsanTypes "github.com/influxdata/telegraf/plugins/inputs/vsphere/vsan/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/types"
)

const (
	vsanNamespace   = "vsan"
	vsanPath        = "/vsanHealth"
	timeFormat      = "Mon, 02 Jan 2006 15:04:05 MST"
	vsanMetricsName = "vsphere_cluster_vsan"
)

var (
	vsanPerformanceManagerInstance = types.ManagedObjectReference{
		Type:  "VsanPerformanceManager",
		Value: "vsan-performance-manager",
	}

	vsanPerfEntityRefIds = []string{
		"cluster-domclient",
		"host-domclient",
		"cache-disk",
		"cache-disk",
		"vsan-vnic-net",
		"vsan-pnic-net",
	}
)

/*
All this cryptic code in formatAndSendVsanMetric is to parse the vsanTypes.VsanPerfEntityMetricCSV type, which has the structure:
{
  "@type": "vim.cluster.VsanPerfEntityMetricCSV",
  "entityRefId": "cluster-domclient:5270dc4d-3594-cc26-b33d-f6be33ddb353",
  "sampleInfo": "2017-06-14 23:10:00,2017-06-14 23:15:00,2017-06-14 23:20:00,2017-06-14 23:25:00,2017-06-14 23:30:00,2017-06-14 23:35:00,2017-06-14 23:40:00,2017-06-14 23:45:00,2017-06-14 23:50:00,2017-06-14 23:55:00,2017-06-15 00:00:00,2017-06-15 00:05:00,2017-06-15 00:10:00",
  "value": [
    {
      "@type": "vim.cluster.VsanPerfMetricSeriesCSV",
      "metricId": {
        "@type": "vim.cluster.VsanPerfMetricId",
        "description": null,
        "group": null,
        "label": "iopsRead",
        "metricsCollectInterval": 300,
        "name": null,
        "rollupType": null,
        "statsType": null
      },
      "threshold": null,
      "values": "1,1,1,1,1,1,1,1,1,1,1,1,1"
    },
		...
		...
	]
}
*/
func formatAndSendVsanMetric(entity vsanTypes.VsanPerfEntityMetricCSV, tags map[string]string, acc telegraf.Accumulator) {
	vals := strings.Split(entity.EntityRefId, ":")
	entityName := vals[0]
	tags["uuid"] = vals[1]
	var timeStamps []string
	for _, t := range strings.Split(entity.SampleInfo, ",") {
		tsParts := strings.Split(t, " ")
		timeStamps = append(timeStamps, fmt.Sprintf("%sT%sZ", tsParts[0], tsParts[1]))
	}
	for _, counter := range entity.Value {
		metricLabel := counter.MetricId.Label
		for i, values := range strings.Split(counter.Values, ",") {
			ts, ok := time.Parse(time.RFC3339, timeStamps[i])
			if ok != nil {
				// can't do much if we couldn't parse time
				log.Printf("D! Failed to parse a timestamp: %s", timeStamps[i])
				continue
			}
			fields := make(map[string]interface{})
			field := fmt.Sprintf("%s_%s", entityName, metricLabel)
			if v, err := strconv.ParseFloat(values, 32); err == nil {
				fields[field] = v
			}
			acc.AddFields(vsanMetricsName, fields, tags, ts)
		}
	}
}

func getAllVsanMetrics(ctx context.Context, vsanClient *soap.Client, cluster *object.ClusterComputeResource, tags map[string]string, acc telegraf.Accumulator) {
	endTime := time.Now()
	startTime := endTime.Add(time.Duration(-5) * time.Minute)
	log.Printf("D! Querying data between: %s -> %s", startTime.Format(timeFormat), endTime.Format(timeFormat))
	for _, entityRefID := range vsanPerfEntityRefIds {
		var querySpecs []vsanTypes.VsanPerfQuerySpec

		spec := vsanTypes.VsanPerfQuerySpec{
			EntityRefId: fmt.Sprintf("%s:*", entityRefID),
			StartTime:   &startTime,
			EndTime:     &endTime,
		}
		querySpecs = append(querySpecs, spec)

		vsanPerfQueryPerf := vsanTypes.VsanPerfQueryPerf{
			This:       vsanPerformanceManagerInstance,
			QuerySpecs: querySpecs,
			Cluster:    cluster.Reference(),
		}
		res, err := methods.VsanPerfQueryPerf(ctx, vsanClient, &vsanPerfQueryPerf)
		if err != nil {
			log.Fatal(err)
		}

		for _, ret := range res.Returnval {
			log.Printf("D! [inputs.vsphere][vSAN]\tSuccessfully Fetched data for Entity ==> %s:%d\n", ret.EntityRefId, len(ret.Value))
			formatAndSendVsanMetric(ret, tags, acc)
		}
	}
}

func getVsanTags(cluster objectRef, vcenter string) map[string]string {
	tags := make(map[string]string)
	tags["vcenter"] = vcenter
	tags["dcname"] = cluster.dcname
	tags["clustername"] = cluster.name
	tags["moid"] = cluster.ref.Value
	tags["source"] = cluster.name
	return tags
}

// CollectVsan invokes the vSAN Performance Manager on the ClusterComputeResource from the input.
func CollectVsan(ctx context.Context, client *vim25.Client, clusterObj objectRef, wg *sync.WaitGroup, vcenter string, acc telegraf.Accumulator) {
	defer wg.Done()
	cluster := object.NewClusterComputeResource(client, clusterObj.ref)
	if clusterName, err := cluster.ObjectName(ctx); err != nil {
		log.Printf("D! [inputs.vsphere][vSAN] Starting vSAN Collection for %s", clusterName)
	}

	tags := getVsanTags(clusterObj, vcenter)
	log.Printf("D! [inputs.vsphere][vSAN] Tags for vSAN: %s", tags)

	// vSAN Client
	vsanClient := client.NewServiceClient(vsanPath, vsanNamespace)
	getAllVsanMetrics(ctx, vsanClient, cluster, tags, acc)
}
