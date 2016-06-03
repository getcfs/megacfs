go build . && fusermount -u /tmp/test && ./cfs /tmp/test


```
Getting Started with CFS:
# install fuse
apt-get install fuse
# install cfs
wget https://github.com/getcfs/cfs-binary-release/releases/download/<latest release>/cfs
echo -e '#!/bin/sh\ncfs mount $1 $2 -o $4 > /dev/null &' > mount.cfs
chmod +x cfs mount.cfs
mv cfs mount.cfs /sbin/
# create the filesystem
cfs -T <token> create -R [iad|aio] -N <fs_name>
# grant access to the filesystem
ifconfig
cfs -T <token> grant -addr <ip> iad://<fs_id>
# mount the filesystem
mkdir -p /mnt/<fs_name>
echo “iad://<fs_id> /mnt/<fs_name> cfs rw 0 0” >> /etc/fstab
mount /mnt/<fs_name>
# optional mount methods
cfs mount iad://<fs_id> /mnt/<fs_name> -o debug
mount -t cfs iad://<fs_id> /mnt/<fs_name>
# unmount the filesystem
umount /mnt/<fs_name>
fusermount -u /mnt/<fs_name>  # use if umount fails


# list all of your file systems
cfs -T <token> list -R [iad|aio]
# show details for a specific file system
cfs -T <token> show iad://<fs id>
# grant access to additional ips
cfs -T <token> grant -addr <ip> iad://<fs id>
# revoke an ip's access
cfs -T <token> revoke -addr <ip> iad://<fs id>

# Both DELETE and UPDATE file system operations are not
#   implemented in at this time
```
