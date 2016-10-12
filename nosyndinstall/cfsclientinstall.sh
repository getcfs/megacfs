#! /bin/bash

# #################
# CFS Client Intall
# #################




apt-get update && apt-get install -y fuse vim screen
update-alternatives --set editor /usr/bin/vim.basic

export REL=0.1.6
export APIIP=162.242.145.109

wget -q https://github.com/getcfs/megacfs/releases/download/$REL/cfs
chmod +x cfs
mv cfs /sbin/

echo "$APIIP	 api.dev.iad.rackfs.com" >> /etc/hosts


# mount command
cfs mount -o debug,allow_others,_netdrv iad:<File System ID> /mnt/cfsdrive

