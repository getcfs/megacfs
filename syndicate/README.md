# syndicate

### setup

You need to make sure:

- /etc/oort exists
- /etc/oort/ring exists
- /etc/oort/ring/value/valuestore.builder exists and has at least one active node
- /etc/oort/ring/value/valuestore.ring exists and has at least one active node
- /etc/oort/ring/group/groupstore.builder exists and has at least one active node
- /etc/oort/ring/group/groupstore.ring exists and has at least one active node
- /etc/oort contains valid server.crt and server.key
- /etc/oort/syndicate.toml contains a valid config like:
```
[valuestore]
Master = true
Salves = ["1.1.1.1", "2.2.2.2"]
NetFilter = ["10.10.10.1/28"]
TierFilter = ["tier.*"]
Port = 8443
CmdCtrlPort = 4443
CertFile = "/etc/oort/server.crt"
KeyFile = "/etc/oort/server.key"
RingDir = "/etc/oort/ring/value"
WeightAssignment = "manual"

[groupstore]
Master = true
Salves = ["1.1.1.1", "2.2.2.2"]
NetFilter = ["10.10.10.1/28"]
TierFilter = ["tier.*"]
Port = 8444
CmdCtrlPort = 4444
CertFile = "/etc/oort/server.crt"
KeyFile = "/etc/oort/server.key"
RingDir = "/etc/oort/ring/group"
WeightAssignment = "self"

#[llamasherpaderpa2000]
#Master = true
#Slaves = []
#NetFilter = ["192.168.1.1/27"]
#Port = 8445
#CmdCtrlPort = 4445
```
- /etc/oort/valuestore.toml contains a valid config like:
```
[CmdCtrlConfig]
ListenAddress = "0.0.0.0:4444"
CertFile = "/etc/oort/server.crt"
KeyFile = "/etc/oort/server.key"
UseTLS = true
Enabled = true

[OortValueStoreConfig]
ListenAddress = "0.0.0.0:6379"
MaxClients = 8192
Profile = false
Debug = false

[ValueStoreConfig]
CompactionInterval = 42
ValueCap = 4194302

[TCPMsgRingConfig]
UseTLS = true
CertFile = "/etc/oort/server.crt"
KeyFile = "/etc/oort/server.key"
```

### temporary dev step (this will go away)

The first time you try and start synd you'll get an error like:

```
root@syndicate1:~/go/src/github.com/getcfs/megacfs/syndicate/synd# go run *.go
2015/09/07 19:56:18 open /etc/oort/syndicate.toml: no such file or directory
2015/09/07 19:56:18 Using default net filter: [10.0.0.0/8 192.168.0.0/16]
2015/09/07 19:56:18 Using default tier filter: [z.*]
2015/09/07 19:56:18 Found /etc/oort/ring/oort.builder, as last builder
2015/09/07 19:56:18 Found /etc/oort/ring/oort.ring, as last ring
2015/09/07 19:56:18 Ring version is: 1439924753845934663
2015/09/07 19:56:18 Attempting to load ring/builder bytes: open /etc/oort/ring/1439924753845934663-oort.builder: no such file or directory
exit status 1
```

To fix this: 

- `cp -av /etc/oort/ring/value/valuestore.builder /etc/oort/ring/value/$THERINGVERSION-valuestore.builder`
- `cp -av /etc/oort/ring/value/valuestore.ring /etc/oort/ring/value/$THERINGVERSION-valuestore.ring`
- `cp -av /etc/oort/ring/group/groupstore.builder /etc/oort/ring/value/$THERINGVERSION-groupstore.builder`
- `cp -av /etc/oort/ring/group/groupstore.ring /etc/oort/ring/value/$THERINGVERSION-groupstore.ring`

If it works you'll see something along the lines of:

```
fhines@47:~/go/src/github.com/getcfs/megacfs/syndicate/synd (master)$ go build . && ./synd -master=true -ring_dir=/etc/oort/ring
2015/09/07 19:58:38 open /etc/oort/syndicate.toml: no such file or directory
2015/09/07 19:58:38 Using default net filter: [10.0.0.0/8 192.168.0.0/16]
2015/09/07 19:58:38 Using default tier filter: [z.*]
2015/09/07 19:58:38 Found /etc/oort/ring/1439924753845934663-oort.builder, as last builder
2015/09/07 19:58:38 Found /etc/oort/ring/1439924753845934663-oort.ring, as last ring
2015/09/07 19:58:38 Ring version is: 1439924753845934663
2015/09/07 19:58:38 !! Running without slaves, have no one to register !!
2015/09/07 19:58:38 Master starting up on 8443...
```

### syndicate-client

go install github.com/getcfs/megacfs/syndicate/syndicate-client


```
    syndicate-client
        Valid commands are:
        version         #print version
        config          #print ring config
        config <nodeid> #uses uint64 id
        search          #lists all
        search id=<nodeid>
        search meta=<metastring>
        search tier=<string> or search tierX=<string>
        search address=<string> or search addressX=<string>
        search any of the above K/V combos
        rm <nodeid>
        set config=./path/to/config
```

### Oort daemons 

Oort daemons either needs a valid SVR record setup or you need to set OORT_SERVICENAME_SYNDICATE_OVERRIDE=127.0.0.1:8443 when running oort-$SERVICENAMEd.

### slaves

aren't working yet

### systemd init script

A working systemd init script is provided in packaging/root/usr/share/synd/systemd/synd.service. To use it
on Debian Jessie `cp packaging/root/usr/share/syndicate/systemd/synd.service /lib/systemd/system`. You can then
stop/start/restart the synd service as usual ala `systemctl start synd`. its setup to capture and log to syslog.

### building packages

should be possible if you have fpm install. try `make packages`

### drone config

