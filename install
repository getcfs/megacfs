#!/usr/bin/env bash
curl -fsSLo /bin/cfs $(curl -s https://api.github.com/repos/getcfs/megacfs/releases | grep -om1 "https://.*/cfs")
echo -e '#!/bin/sh\n/bin/cfs mount -o $4 $1 $2 &' > /sbin/mount.cfs
chmod +x /bin/cfs /sbin/mount.cfs