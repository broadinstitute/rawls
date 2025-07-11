
# rawls metrics reporting

Rawls reports its metrics to prometheus via prom/statsd-exporter.

## see metrics locally
1. Create a file named `statsd_mapping.yml` to allow all metrics with the following content:
   ```yaml
   mappings:
     - match: "*"
       name: "$1"
       labels: {}
   ```
2. Start a container using the docker image prom/statsd-exporter using the mapping file you just created and exposing ports 9102 and 9125:
   ```
   docker run -p 9102:9102 -p 9125:9125 -p 9125:9125/udp \
        -v $PWD/statsd_mapping.yml:/tmp/statsd_mapping.yml \
        prom/statsd-exporter --statsd.mapping-config=/tmp/statsd_mapping.yml
   ```
3. In config/rawls.conf, change the value of metrics.enabled to true, and the value of metrics.reporters.host to "host.docker.internal": 
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
4. Start rawls via docker-rsync-local.sh
5. Reported metrics can be seen by running `curl http://localhost:9102/metrics`

To capture metrics in live environments, you will also need to update the statsd mappings in terra-helmfile. These
are defined [here](https://github.com/broadinstitute/terra-helmfile/blob/master/values/app/rawls/live.yaml.gotmpl).
You can iterate on those mappings locally by using the `statsd_mapping.yml` file created in step 1 above.
