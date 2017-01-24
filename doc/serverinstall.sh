#!/usr/bin/env bash
curl -fsSLo /usr/local/bin/cfsd $(curl -s https://api.github.com/repos/getcfs/megacfs/releases | grep -om1 "https://.*/cfsd")
curl -fsSLo /usr/local/bin/cfsadm $(curl -s https://api.github.com/repos/getcfs/megacfs/releases | grep -om1 "https://.*/cfsadm")
curl -fsSLo /usr/local/bin/oort-cli $(curl -s https://api.github.com/repos/getcfs/megacfs/releases | grep -om1 "https://.*/oort-cli")
curl -fsSLo /usr/local/bin/ring $(curl -s https://api.github.com/repos/getcfs/megacfs/releases | grep -om1 "https://.*/ring")
chmod +x /usr/local/bin/cfsadm /usr/local/bin/cfsd /usr/local/bin/oort-cli /usr/local/bin/ring 

