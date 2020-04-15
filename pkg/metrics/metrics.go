package metrics

import (
	kitmetrics "github.com/go-kit/kit/metrics"
	m "github.com/zhiqiangxu/util/metrics"
)

const (
	// RequestCount is name for request count
	RequestCount = "request_count"
	// RequestLatency is name for request latency
	RequestLatency = "request_latency"
)

var (
	// RequestLatencyMetric for RequestLatency
	RequestLatencyMetric kitmetrics.Histogram
	// RequestCountMetric for RequestCount
	RequestCountMetric kitmetrics.Counter
)

func init() {
	m.RegisterCounter(RequestCount, []string{"method", "error"})
	m.RegisterHist(RequestLatency, []string{"method", "error"})
	RequestLatencyMetric = m.GetHist(RequestLatency)
	RequestCountMetric = m.GetCounter(RequestCount)
}
