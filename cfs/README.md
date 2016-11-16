Getting Started Guide
=====================

Default formic port is 8445

environment | address | comment
----------- | ------- | -------
aio | 127.0.0.1 | All in One
iad | api.ea.iad.rackfs.com | Original EA syndicate based
rpc | rpc.ea.iad.rackfs.com:8445 | public dns for rpc ea testing
dev | api.dev.iad.rackfs.com | Development syndicate based


Quick Start
-----------

1) Install FUSE
```bash
apt-get install fuse
```

2) Install CFS
```bash
wget https://github.com/getcfs/megacfs/releases/download/<latest_release>/cfs
echo -e '#!/bin/sh\ncfs mount -o $4 $1 $2 > /dev/null &' > mount.cfs  # create the mount helper script
chmod +x cfs mount.cfs   # mark them executable
mv cfs mount.cfs /sbin/  # place them on the path
```

3) Configure the CFS Client
```bash
cfs configure  # requires a valid Rackspace Cloud region, username and apikey
```

4) Create a Filesystem
```bash
cfs create myfs  # returns the fsid
```

5) Grant Access to the Filesystem
```bash
ifconfig               # to get the service net ip
cfs grant <ip> <fsid>  # allows the filesystem to mounted from this ip
```

6) Mount the Filesystem
```bash
mkdir -p /mnt/myfs                                                # create the mountpoint
echo "iad:<fsid> /mnt/myfs cfs rw,allow_other 0 0" >> /etc/fstab  # add filesystem to /etc/fstab
mount /mnt/myfs                                                   # mount the filesystem
```
