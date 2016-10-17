Prometheus

Get Prometheus
    wget -q https://github.com/prometheus/prometheus/releases/download/v1.2.1/prometheus-1.2.1.linux-amd64.tar.gz
    wget -q https://github.com/prometheus/node_exporter/releases/download/0.12.0/node_exporter-0.12.0.linux-amd64.tar.gz


mkdir -p /data/prometheus

tar xvfz prometheus-*.tar.gz
tar xvfz node_exporter-*.tar.gz
cd prometheus


base config file in prometheus.yml

global:
  scrape_interval:     15s # By default, scrape targets every 15 seconds.

  # Attach these labels to any time series or alerts when communicating with
  # external systems (federation, remote storage, Alertmanager).
  external_labels:
    monitor: 'codelab-monitor'

# A scrape configuration containing exactly one endpoint to scrape:
# Here it's Prometheus itself.
scrape_configs:
  # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: 'prometheus'

    # Override the global default and scrape targets from this job every 5 seconds.
    scrape_interval: 5s

    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'valuestore'
    scrape_interval: 5s
    static_configs:
      - targets: ['162.242.145.109:9300', '23.253.72.110:9300']

  - job_name: 'groupstore'
    scrape_interval: 5s
    static_configs:
      - targets: ['162.242.145.109:9200', '23.253.72.110:9200']
  
  - job_name: 'api'
    scrape_interval: 5s
    static_configs:
      - targets: ['162.242.145.109:9100', '23.253.72.110:9100']