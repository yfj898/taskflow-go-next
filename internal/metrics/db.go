package metrics

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

// RegisterDBPool exposes database/sql connection-pool pressure. These metrics
// make pool tuning evidence-based instead of increasing MaxOpenConns blindly.
func RegisterDBPool(db *sql.DB) {
	prometheus.MustRegister(&dbPoolCollector{db: db})
}

type dbPoolCollector struct {
	db *sql.DB
}

var (
	dbOpen        = prometheus.NewDesc("taskflow_db_open_connections", "Open database connections.", nil, nil)
	dbUse         = prometheus.NewDesc("taskflow_db_in_use_connections", "Database connections currently in use.", nil, nil)
	dbIdle        = prometheus.NewDesc("taskflow_db_idle_connections", "Idle database connections.", nil, nil)
	dbWait        = prometheus.NewDesc("taskflow_db_wait_total", "Times callers waited for a database connection.", nil, nil)
	dbWaitSeconds = prometheus.NewDesc(
		"taskflow_db_wait_duration_seconds_total",
		"Cumulative time spent waiting for a database connection.",
		nil, nil,
	)
)

func (c *dbPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dbOpen
	ch <- dbUse
	ch <- dbIdle
	ch <- dbWait
	ch <- dbWaitSeconds
}

func (c *dbPoolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.db.Stats()
	ch <- prometheus.MustNewConstMetric(dbOpen, prometheus.GaugeValue, float64(s.OpenConnections))
	ch <- prometheus.MustNewConstMetric(dbUse, prometheus.GaugeValue, float64(s.InUse))
	ch <- prometheus.MustNewConstMetric(dbIdle, prometheus.GaugeValue, float64(s.Idle))
	ch <- prometheus.MustNewConstMetric(dbWait, prometheus.CounterValue, float64(s.WaitCount))
	ch <- prometheus.MustNewConstMetric(dbWaitSeconds, prometheus.CounterValue, s.WaitDuration.Seconds())
}
