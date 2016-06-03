oort

### A fast persistent clustered key/value store

"It's not the future" ~ wreese

### Installation

* go get -u github.com/getcfs/megacfs/oort-valued
* go get -u github.com/getcfs/megacfs/oort-groupd
* go install github.com/getcfs/megacfs/oort/oort-valued
* go install github.com/getcfs/megacfs/oort/oort-groupd
* go install github.com/getcfs/megacfs/oort/oort-cli
* mkdir -p /var/lib/oort-value/ring /var/lib/oort-group/ring
* mkdir -p /var/lib/oort-value/data /var/lib/oort-group/data
* If you'll be using the CmdCtrl interface you'll need to deploy your SSL key/crt to /var/lib/oort-value or whatever path you've specified in the ring config.

### Oort Daemons/Binaries/Backends

* oort-valued - The grpc speaking value storage daemon
* oort-groupd - The grpc speaking group storage daemon
* oort-bench - The grpc speaking simple benchmark utility
* oort-cli - A small redis-cli like utility for performing basic ops against the store's

# Testing out a POC using cfs -> formic -> oort-valued | oort-groupd

### syndicate

Oort-${service}d obtains configuration info and the rings by communicate with a running syndicate server (synd).
To discover the syndicate server it either attempts to use SRV records. The SRV record should be structured as follows:

```_oortservicename-syndicate._tcp.iad3.velocillama.com. 300 IN SRV 1 1 8443 syndicate1.iad3.velocillama.com.```

The service needs to be "syndicate", "proto" should be tcp. The rest of the service address (the iad3.velocillama.com portion) is derived from the systems local hostname. So if the local hostname is "devmachine.iad3.domain.com" and you're running the "value" (oort-valued) service the service record for it would be `_value-syndicate._tcp.iad3.domain.com`. The address and port target should be the address and port of your running synd instance. If you're running a local dev instance and don't have or want to setup a DNS record you can also use "env OORT_$(service)_SYNDICATE_OVERRIDE=127.0.0.1:$(serviceport)" to fake a return SRV record.

Configuration is applied in this order:

1. Main ring config as returned by syndicate
2. Per node ring config as returned by syndicate
3. Env Var's specified /etc/default/oort_servicenamed (which is sourced by the systemd service script)

While not recommended, you can by pass SRV lookups and Syndicate usage completely by setting the following env vars:

- `OORT_VALUED_SKIP_SRV=true`
- `OORT_VALUED_LISTEN_ADDRESS=something`
- `OORT_VALUED_LOCALID=1010101010101`
- `OORT_VALUED_RING_FILE=/path/to/ring`
