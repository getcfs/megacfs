Getting Started Guide
=====================

Quick Start
-----------

1) Install FUSE
```bash
apt-get install fuse
```

2) Install the CFS Client
```bash
wget https://github.com/getcfs/cfs-binary-release/releases/download/<latest release>/cfs
chmod +x cfs   # make it executable
mv cfs /sbin/  # put it on the path
```

3) Configure the CFS Client
```bash
cfs configure  # requires a valid region, username and apikey
```

4) Create a Filesystem
```bash
cfs create myfs  # returns the fsid
```

5) Grant Access to the Filesystem
```bash
ifconfig               # to get the service net ip
cfs grant <ip> <fsid>  # allows this filesystem to mounted from this ip
```

6) Mount the Filesystem
```bash
mkdir -p /mnt/myfs          # create the mountpoint  
cfs mount <fsid> /mnt/myfs  # mount the filesystem
```



Mounting via /etc/fstab
-----------------------

1) Create the helper script for mount
```bash
echo -e '#!/bin/sh\ncfs mount -0 $4 $1 $2 > /dev/null &' > mount.cfs
chmod +x mount.cfs
mv mount.cfs /sbin/
```

2) Add the filesystem to /etc/fstab
```bash
echo "iad:<fsid> /mnt/myfs cfs rw,allow_other 0 0" >> /etc/fstab
```

3) Mount and unmount using the mountpoint
```bash
mount /mnt/myfs
umount /mnt/myfs
```
