

export VERSION=0.13.0
mkdir -p ~/Prometheus/node_exporter
cd ~/Prometheus/node_exporter

wget https://github.com/prometheus/node_exporter/releases/download/v$VERSION/node_exporter-$VERSION.linux-amd64.tar.gz -O ~/Downloads/node_exporter-$VERSION.linux-amd64.tar.gz
tar -xvzf ~/Downloads/node_exporter-$VERSION.linux-amd64.tar.gz

sudo ln -s ~/Prometheus/node_exporter/node_exporter /usr/bin


vim /etc/init/node_exporter.conf
    # Run node_exporter

    start on startup

    script
        /usr/bin/node_exporter
    end script

sudo service node_exporter start


vim ~/prometheus/prometheus.yml

...
scrape_configs:
  - job_name: "node"
    scrape_interval: "15s"
    target_groups:
    - targets: ['localhost:9100']
...


https://github.com/prometheus/node_exporter/releases/download/v0.13.0/node_exporter-0.13.0.linux-arm64.tar.gz

