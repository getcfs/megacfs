#! /bin/bash
#
# CFS Server Installation Guide (without syndicate)
# Node 1

# ###########
# Create NODE
# ###########

export NODE=cfst3node1.rackfs.com
export KEYP=letterj-rax-pubkey
export PROFILE=doomsdaydfw


rack servers instance create --name=$NODE --flavor-id=general1-8 --image-name="Debian 8 (Jessie) (PVHVM)" --keypair=$KEYP --profile=$PROFILE

# ##########################################################################################################################################


# ###########
# Run on NODE
# ###########

export REL=0.1.6

apt-get update && apt-get install -y vim screen
update-alternatives --set editor /usr/bin/vim.basic
mkdir -p /etc/cfsd && mkdir -p /var/lib/cfsd

# Create and push out keys
openssl req -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/cfsd/server.key -out /etc/cfsd/server.crt
cp /etc/cfsd/server.key /etc/cfsd/client.key
cp /etc/cfsd/server.crt /etc/cfsd/client.crt

# Download the systemd service files

wget -q https://github.com/getcfs/megacfs/releases/download/$REL/oort-valued
wget -q https://github.com/getcfs/megacfs/releases/download/$REL/oort-groupd
wget -q https://github.com/getcfs/megacfs/releases/download/$REL/formicd
wget -q https://github.com/getcfs/megacfs/releases/download/$REL/oort-cli
wget -q https://github.com/getcfs/megacfs/releases/download/$REL/ring
chmod 777 oort-valued oort-groupd formicd oort-cli ring
mv oort-valued oort-groupd formicd oort-cli ring /usr/local/bin/

# Create ring with 1 node
export nodeip=$(ip addr show eth0 | grep -Po 'inet \K[\d.]+')
ring valuestore.builder create replicas=1
ring valuestore.builder add address0=$nodeip:8001 address1=$nodeip:8002 address2=$nodeip:8003
ring valuestore.builder ring
mv valuestore.ring /etc/cfsd
ring groupstore.builder create replicas=1
ring groupstore.builder add address0=$nodeip:8011 address1=$nodeip:8012 address2=$nodeip:8013 
ring groupstore.builder ring
mv groupstore.ring /etc/cfsd
mkdir -p /etc/cfsd/ring
ln -s /etc/cfsd/valuestore.ring /etc/cfsd/ring/valuestore.ring
ln -s /etc/cfsd/groupstore.ring /etc/cfsd/ring/groupstore.ring


# GET the systemd service and toml files
wget -q https://raw.githubusercontent.com/getcfs/megacfs/master/nosyndinstall/etc/cfsd/cfsd.toml
chmod 777 cfsd.toml
mv cfsd.toml /etc/cfsd/
wget -q https://raw.githubusercontent.com/getcfs/megacfs/master/nosyndinstall/lib/systemd/system/formicd.service
wget -q https://raw.githubusercontent.com/getcfs/megacfs/master/nosyndinstall/lib/systemd/system/oort-groupd.service
wget -q https://raw.githubusercontent.com/getcfs/megacfs/master/nosyndinstall/lib/systemd/system/oort-valued.service
chmod 777 formicd.service oort-valued.service oort-groupd.service
mv formicd.service oort-groupd.service oort-valued.service /lib/systemd/system/


wget -q https://raw.githubusercontent.com/getcfs/megacfs/master/nosyndinstall/etc/default/cfsd
chmod 777 cfsd
mv cfsd /etc/default/

# Set local variables in /etc/default/cfsd
sed -i -e "s/NODENUM/1/g" /etc/default/cfsd
export IDFROMVALUERING=$(ring /etc/cfsd/valuestore.ring node | grep ID | awk '{ print $2 }')
sed -i -e "s/IDFROMVALUERING/$IDFROMVALUERING/g" /etc/default/cfsd
export IDFROMGROUPRING=$(ring /etc/cfsd/groupstore.ring node | grep ID | awk '{ print $2 }')
sed -i -e "s/IDFROMGROUPRING/$IDFROMGROUPRING/g" /etc/default/cfsd


# Enable and Start Services
systemctl daemon-reload
systemctl enable oort-valued oort-groupd formicd
systemctl restart oort-valued oort-groupd formicd