Cloud File System (CFS) Client Application
==========================================

Target operating system is Linux.  
Other operating systems to follow.


Quick Start
-----------

1) Install FUSE
```bash
sudo apt-get install fuse   # Debian or Ubuntu
sudo yum install fuse       # CentOS
sudo dnf install fuse       # Fedora
```

CFS requires FUSE version 2.6 or greater, which you can verify with the following command.
```
$ fusermount -V
fusermount version: 2.9.3
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
cfs configure  # requires an Auth URL, Username, Password, and CFS Server
```

4) Create a Filesystem
```bash
cfs create myfs  # returns the fsid
```

5) Grant Access to the Filesystem
```bash
cfs grant <fsid>  # allows the filesystem to be mounted from this client's ip
```

6) Mount the Filesystem
```bash
mkdir -p /mnt/myfs                                                            # create the mountpoint
echo "<cfs_server_ip>:<fsid> /mnt/myfs cfs rw,allow_other 0 0" >> /etc/fstab  # add filesystem to /etc/fstab
mount /mnt/myfs                                                               # mount the filesystem
```
