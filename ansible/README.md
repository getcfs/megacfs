# Install Client

```
curl -fsSL https://raw.githubusercontent.com/getcfs/megacfs/master/install.sh | sudo sh

# auth point, user, token, cfsd endpoint
echo "http://192.168.0.1:5000/ admin 3a9778240325e5904cb427aedf80f9b326e086a0574a810964404535110e5 192.168.1.4" | cfs configure

cfs list
ID                                      Name

cfs create weloveyoujay
ID: 68270f8b-c1b0-4f84-9fc7-c58b2caa78f3

cfs grant 68270f8b-c1b0-4f84-9fc7-c58b2caa78f3
192.168.1.4

mkdir -p /mnt/weloveyoujay

cfs mount -o debug 192.168.1.4:68270f8b-c1b0-4f84-9fc7-c58b2caa78f3 /mnt/weloveyoujay &
[1] 20133
2016/12/14 20:36:55 Inside InitFs

mount
none on /proc/xen type xenfs (rw)
/dev/xvda1 on / type ext4 (rw,noatime,errors=remount-ro,barrier=0)
proc on /proc type proc (rw,noexec,nosuid,nodev)
sysfs on /sys type sysfs (rw,noexec,nosuid,nodev)
none on /sys/fs/cgroup type tmpfs (rw)
none on /sys/fs/fuse/connections type fusectl (rw)
none on /sys/kernel/debug type debugfs (rw)
none on /sys/kernel/security type securityfs (rw)
udev on /dev type devtmpfs (rw,mode=0755)
devpts on /dev/pts type devpts (rw,noexec,nosuid,gid=5,mode=0620)
tmpfs on /run type tmpfs (rw,noexec,nosuid,size=10%,mode=0755)
none on /run/lock type tmpfs (rw,noexec,nosuid,nodev,size=5242880)
none on /run/shm type tmpfs (rw,nosuid,nodev)
none on /run/user type tmpfs (rw,noexec,nosuid,nodev,size=104857600,mode=0755)
none on /sys/fs/pstore type pstore (rw)
systemd on /sys/fs/cgroup/systemd type cgroup (rw,noexec,nosuid,nodev,none,name=systemd)
cfs on /mnt/weloveyoujay type fuse.cfs (rw,nosuid,nodev,default_permissions)

echo "weloveyoujay" > /mnt/weloveyoujay/weloveyoujay.txt
2016/12/14 20:36:55 Inside handleGetattr
2016/12/14 20:36:55 Getattr [ID=0x2 Node=0x1 Uid=0 Gid=0 Pid=17168] 0x0 fl=0
2016/12/14 20:36:55 Getattr valid=5s ino=1 size=0 mode=drwxrwxr-x
2016/12/14 20:36:55 Inside handleLookup
2016/12/14 20:36:55 Running Lookup for weloveyoujay.txt
2016/12/14 20:36:55 Lookup [ID=0x3 Node=0x1 Uid=0 Gid=0 Pid=17168] "weloveyoujay.txt"
2016/12/14 20:36:55 ENOENT Lookup(weloveyoujay.txt)
2016/12/14 20:36:55 Inside handleCreate
2016/12/14 20:36:55 Create [ID=0x4 Node=0x1 Uid=0 Gid=0 Pid=17168] "weloveyoujay.txt" fl=OpenWriteOnly+OpenCreate+OpenTruncate mode=-rw-r--r-- umask=-----w--w-
2016/12/14 20:36:55 Inside handleFlush
2016/12/14 20:36:55 Inside handleGetxattr
2016/12/14 20:36:55 Getxattr [ID=0x6 Node=0xcf953d1d4bd34c01 Uid=0 Gid=0 Pid=17168] "security.capability" 0 @0
2016/12/14 20:36:55 Inside handleWrite
2016/12/14 20:36:55 Writing 13 bytes at offset 0
2016/12/14 20:36:55 Write [ID=0x7 Node=0xcf953d1d4bd34c01 Uid=0 Gid=0 Pid=17168] 0x0 13 @0 fl=0 lock=0 ffl=OpenWriteOnly
2016/12/14 20:36:55 Inside handleFlush
r2016/12/14 20:36:55 Inside handleRelease

root@cfsnode01:~# cat /mnt/weloveyoujay/weloveyoujay.txt 
2016/12/14 20:37:07 Inside handleGetattr
2016/12/14 20:37:07 Getattr [ID=0xa Node=0x1 Uid=0 Gid=0 Pid=20316] 0x0 fl=0
2016/12/14 20:37:07 Getattr valid=5s ino=1 size=0 mode=drwxrwxr-x
2016/12/14 20:37:07 Inside handleLookup
2016/12/14 20:37:07 Running Lookup for weloveyoujay.txt
2016/12/14 20:37:07 Lookup [ID=0xb Node=0x1 Uid=0 Gid=0 Pid=20316] "weloveyoujay.txt"
2016/12/14 20:37:07 Lookup 0xcf953d1d4bd34c01 gen=0 valid=5s attr={valid=5s ino=14957928933416455169 size=13 mode=-rw-r--r--}
2016/12/14 20:37:07 Inside handleOpen
2016/12/14 20:37:07 Open [ID=0xc Node=0xcf953d1d4bd34c01 Uid=0 Gid=0 Pid=20316] dir=false fl=OpenReadOnly
2016/12/14 20:37:07 Open 0x0 fl=OpenKeepCache
2016/12/14 20:37:07 Inside handleRead
2016/12/14 20:37:07 Read [ID=0xd Node=0xcf953d1d4bd34c01 Uid=0 Gid=0 Pid=20316] 0x0 4096 @0x0 dir=false fl=0 lock=0 ffl=OpenReadOnly
weloveyoujay
2016/12/14 20:37:07 Inside handleGetattr
2016/12/14 20:37:07 Getattr [ID=0xe Node=0xcf953d1d4bd34c01 Uid=0 Gid=0 Pid=20316] 0x0 fl=GetattrFh
2016/12/14 20:37:07 Getattr valid=5s ino=14957928933416455169 size=13 mode=-rw-r--r--
2016/12/14 20:37:07 Inside handleFlush
2016/12/14 20:37:07 Inside handleRelease

umount /mnt/weloveyoujay
[1]+  Done                    cfs mount -o debug 192.168.1.4:68270f8b-c1b0-4f84-9fc7-c58b2caa78f3 /mnt/weloveyoujay

```

## Intall Server

*On Node 1*

Added team's public keys

```
apt-get update
apt-get upgrade -V --yes --force-yes
apt-get install -y vim screen 
update-alternatives --set editor /usr/bin/vim.basic
wget -q https://github.com/getcfs/megacfs/releases/download/pre-0.1.10/cfsd
wget -q https://github.com/getcfs/megacfs/releases/download/pre-0.1.10/cfsadm
wget -q https://github.com/getcfs/megacfs/releases/download/pre-0.1.10/oort-cli
wget -q https://github.com/getcfs/megacfs/releases/download/pre-0.1.10/ring
chmod 777 cfsd oort-cli ring 
mv cfsadm cfsd oort-cli ring /usr/local/bin/

wget -q https://raw.githubusercontent.com/letterj/megacfs/master/cfsd/packaging/root/usr/share/cfsd/init/cfsd.conf
chmod 777 cfs.conf
mv cfsd.conf /etc/init/

cfsadm init
cfsadm add [IP] <IP> ....
```
