// internal/metrics/metrics.go
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// We define all our metrics as package-level variables.
// They are exported so other packages (like `server`) can use them.
var (
	CommandsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "redis_commands_processed_total",
		Help: "The total number of processed commands, labeled by command name.",
	}, []string{"command"})

	ConnectedClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "redis_connected_clients",
		Help: "Current number of connected clients.",
	})
)

// The init() function is a special Go function that runs automatically
// when this package is first imported. The promauto package uses this
// mechanism to register the metrics with the default Prometheus registry.
// This is why we don't need to change anything in main.go!