# CFS for RPC-O

## Install cfsd on servers

### Parameters

    cfsd_server01_public = 127.0.0.1
    cfsd_server01_priviate = 127.0.0.1
    authurl = http://127.0.0.1:5000
    user = admin
    password = admin

### Commands

```
apt-get update
apt-get upgrade -V --yes --force-yes
apt-get install -y vim screen 
update-alternatives --set editor /usr/bin/vim.basic
curl -fsSLo /usr/local/bin/cfsd $(curl -s https://api.github.com/repos/getcfs/megacfs/releases | grep -om1 "https://.*/cfsd")
curl -fsSLo /usr/local/bin/cfsadm $(curl -s https://api.github.com/repos/getcfs/megacfs/releases | grep -om1 "https://.*/cfsadm")
curl -fsSLo /usr/local/bin/oort-cli $(curl -s https://api.github.com/repos/getcfs/megacfs/releases | grep -om1 "https://.*/oort-cli")
curl -fsSLo /usr/local/bin/ring $(curl -s https://api.github.com/repos/getcfs/megacfs/releases | grep -om1 "https://.*/ring")
chmod +x /usr/local/bin/cfsadm /usr/local/bin/cfsd /usr/local/bin/oort-cli /usr/local/bin/ring 
wget -q https://raw.githubusercontent.com/letterj/megacfs/master/allinone/packaging/root/usr/share/cfsd/init/cfsd.conf
chmod 777 cfs.conf
mv cfsd.conf /etc/init/

cfsadm init
// repeat for each server
cfsadm add cfsd_server01_pub cfsd_server1_priviate
```

create file: */etc/cfsd/cfsd.conf* 

```
AUTH_URL=$authurl
AUTH_USER=$user
AUTH_PASSWORD=$password
```

data storage:  */var/lib/cfsd*
Just create a symlink if you wish to change the location to a separate location


## Install client

### Parameters:
  fsid = 1234
  formicip = 127.0.0.1:
  token = <blah>

### Commands:

```
export OS_TOKEN = $token
yum install -y fuse
curl -fsSLo /bin/cfs $(curl -s https://api.github.com/repos/getcfs/megacfs/releases | grep -om1 "https://.*/cfs")
echo -e '#!/bin/sh\n/bin/cfs mount -o $4 $1 $2 &' > /sbin/mount.cfs
chmod +x /bin/cfs /sbin/mount.cfs
mkdir -p /mnt/k8s_cfs
echo '$fomicip:$fsid /mnt/k8s_cfs cfs allow_other,_netdev 0 0' >> /etc/fstab
mount /mnt/k8s_cfs
cfs grant $fsid
```

### Managed Kube

Mount the file system on all kube hosts  "/mnt/k8s_cfs

Example yaml file:  
```
apiVersion: v1
kind: ReplicationController
metadata:
  name: my-nginx
spec:
  replicas: 2
  template:
    metadata:
      labels:
        run: my-nginx
    spec:
      containers:
      - name: my-nginx
        image: nginx
        ports:
        - containerPort: 80
        volumeMounts:
        - mountPath: /data-cfs
          name: cfs-volume
      volumes:
      - name: cfs-volume
        hostPath:
          path: /mnt/k8s_cfs
```

### Open Questions

cfs File System Create?  Where/Who?
    How File System(s) for each Kub cluster?

Key Stone user to use for cfs
    Who, What, etc creates it?

Network?

