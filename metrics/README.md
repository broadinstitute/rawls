
# rawls metrics reporting

Rawls reports its metrics to prometheus via prom/statsd-exporter.

## see metrics locally
1. Start a container using the docker image prom/statsd-exporter, exposing ports 9102 and 9125
2. In config/rawls.conf, change the value of metrics.enabled to true, and the value of metrics.reporters.host to "host.docker.internal": 
  ```yaml
metrics {
  enabled = true
  prefix = "dev.firecloud.rawls"
  includeHostname = false
  reporters {
    # Direct metrics to statsd-exporter sidecar to send to Prometheus
    statsd-sidecar {
      host = "host.docker.internal"
      port = 9125
      period = 30s
    }
  }
}
  ```
3. Start rawls via docker-rsync-local.sh
4. Reported metrics can be seen by running `curl http://localhost:9102/metrics` 
