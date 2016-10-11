#! /bin/bash
#
# CFS Server Installation Guide (without syndicate)
# Node 1

export rel=1.5
export node01=cfst3node1.rackfs.com
export keypair=letterj-rax-pubkey
export profile=projectdoomdfw


./rack servers instance create --name=$NODE1 --flavor-id=general1-8 --image-name="Debian 8 (Jessie) (PVHVM)" --keypair=$KEYP --profile=$PROFILE
apt-get update && apt-get install -y git vim


mkdir -p /etc/cfsd && mkdir -p /var/lib/cfsd

# Create and push out keys
openssl req -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/cfsd/server.key -out /etc/cfsd/server.crt
cp /etc/cfsd/server.key /etc/cfsd/client.key
cp /etc/cfsd/server.crt /etc/cfsd/client.crt

# Create ring with 1 node
export nodeip1=23.253.119.78
ring valuestore.builder create replicas=1
ring valuestore.builder add address0=$nodeip1:8001 address1=$nodeip1:8002 address2=$nodeip1:8003
ring valuestore.builder ring
mv valuestore.ring /etc/cfsd
ring groupstore.builder create replicas=1
ring groupstore.builder add address0=$nodeip1:8011 address1=$nodeip1:8012 address2=$nodeip1:8013 
ring groupstore.builder ring
mv groupstore.ring /etc/cfsd
mkdir -p /etc/cfsd/ring
ln -s /etc/cfsd/valuestore.ring /etc/cfsd/ring/valuestore.ring
ln -s /etc/cfsd/groupstore.ring /etc/cfsd/ring/groupstore.ring

# Download the systemd service files
wget -q https://github.com/getcfs/megacfs/releases/download/$rel/oort-valued
wget -q https://github.com/getcfs/megacfs/releases/download/$rel/oort-groupd
wget -q https://github.com/getcfs/megacfs/releases/download/$rel/formicd
wget -q https://github.com/getcfs/megacfs/releases/download/$rel/oort-cli
wget -q https://github.com/getcfs/megacfs/releases/download/$rel/ring
chmod 777 oort-valued oort-groupd formicd oort-cli ring
mv oort-valued oort-groupd formicd oort-cli ring /usr/local/bin/

# GET the systemd service and toml files
wget -q https://github.com/letterj/megacfs/blob/master/nosyndinstall/etc/cfsd/cfsd.toml
chmod 777 cfsd.tomal
mv cfsd.toml /etc/cfsd/
wget -q https://github.com/letterj/megacfs/blob/master/nosyndinstall/lib/system/systemd/formicd.service
wget -q https://github.com/letterj/megacfs/blob/master/nosyndinstall/lib/system/systemd/oort-groupd.service
wget -q https://github.com/letterj/megacfs/blob/master/nosyndinstall/lib/system/systemd/oort-valued.service
chmod 777 formicd.service oort-valued.service oort-groupd.service
mv formicd.service oort-groupd.service oort-valued.service /lib/system/systemd/


# Set in /etc/default/cfsd
ring /etc/cfsd/valuestore.ring node  # to get OORT_VALUE_LOCALID
ring /etc/cfsd/groupstore.ring node  # to get OORT_GROUP_LOCALID

wget -q https://github.com/letterj/megacfs/blob/master/nosyndinstall/etc/default/cfsd
# Change these variables
IDFROMVALUERING
IDFROMGROUPRING
NODENUM

# Enable and Start Services
systemctl daemon-reload
systemctl enable oort-valued oort-groupd formicd
systemctl restart oort-valued oort-groupd formicd