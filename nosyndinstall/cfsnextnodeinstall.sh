#! /bin/bash
#
# CFS Server Installation Guide (without syndicate)
# Node 1

# ###########
# Create NODE
# ###########

export NODE=cfst3node2.rackfs.com
export KEYP=letterj-rax-pubkey
export PROFILE=doomsdaydfw


rack servers instance create --name=$NODE --flavor-id=general1-8 --image-name="Debian 8 (Jessie) (PVHVM)" --keypair=$KEYP --profile=$PROFILE

# ##########################################################################################################################################


# ###########
# Run on NODE One 
# ###########

export RELEASE=0.1.7
export NODENUM=2
export nodeip2=23.253.72.110

ssh $nodeip2 "apt-get update && apt-get install -y vim screen"
ssh $nodeip2 "update-alternatives --set editor /usr/bin/vim.basic"
ssh $nodeip2 "mkdir -p /etc/cfsd && mkdir -p /var/lib/cfsd && mkdir -p /etc/cfsd/ring"


# Create and push out keys
# From node1 
scp /etc/cfsd/server.key   $nodeip2:/etc/cfsd/server.key
scp /etc/cfsd/server.crt   $nodeip2:/etc/cfsd/server.crt
ssh $nodeip2 "ln -sf /etc/cfsd/server.key /etc/cfsd/client.key"
ssh $nodeip2 "ln -sf /etc/cfsd/server.crt /etc/cfsd/client.crt"

# Download the systemd service files
ssh $nodeip2 "wget -q https://github.com/getcfs/megacfs/releases/download/$RELEASE/oort-valued"
ssh $nodeip2 "wget -q https://github.com/getcfs/megacfs/releases/download/$RELEASE/oort-groupd"
ssh $nodeip2 "wget -q https://github.com/getcfs/megacfs/releases/download/$RELEASE/formicd"
ssh $nodeip2 "wget -q https://github.com/getcfs/megacfs/releases/download/$RELEASE/oort-cli"
ssh $nodeip2 "wget -q https://github.com/getcfs/megacfs/releases/download/$RELEASE/ring"
ssh $nodeip2 "chmod 777 oort-valued oort-groupd formicd oort-cli ring"
ssh $nodeip2 "mv oort-valued oort-groupd formicd oort-cli ring /usr/local/bin/"


# GET the systemd service and toml files
ssh $nodeip2 "wget -q https://raw.githubusercontent.com/getcfs/megacfs/master/nosyndinstall/etc/cfsd/cfsd.toml"
ssh $nodeip2 "chmod 777 cfsd.toml"
ssh $nodeip2 "mv cfsd.toml /etc/cfsd/"
ssh $nodeip2 "wget -q https://raw.githubusercontent.com/getcfs/megacfs/master/nosyndinstall/lib/systemd/system/formicd.service"
ssh $nodeip2 "wget -q https://raw.githubusercontent.com/getcfs/megacfs/master/nosyndinstall/lib/systemd/system/oort-groupd.service"
ssh $nodeip2 "wget -q https://raw.githubusercontent.com/getcfs/megacfs/master/nosyndinstall/lib/systemd/system/oort-valued.service"
ssh $nodeip2 "chmod 777 formicd.service oort-valued.service oort-groupd.service"
ssh $nodeip2 "mv formicd.service oort-groupd.service oort-valued.service /lib/systemd/system/"


# Add node2 to the ring with 1 node
export nodeip=$(ip addr show eth0 | grep -Po 'inet \K[\d.]+')
ring valuestore.builder set-replicas $REPLICAS
ring valuestore.builder add address0=$nodeip2:8001 address1=$nodeip2:8002 address2=$nodeip2:8003
ring valuestore.builder ring
ring groupstore.builder set-replicas $REPLICAS
ring groupstore.builder add address0=$nodeip2:8011 address1=$nodeip2:8012 address2=$nodeip2:8013 
ring groupstore.builder ring

mv valuestore.ring /etc/cfsd
mv groupstore.ring /etc/cfsd
ln -sf /etc/cfsd/valuestore.ring /etc/cfsd/ring/valuestore.ring
ln -sf /etc/cfsd/groupstore.ring /etc/cfsd/ring/groupstore.ring

# Copy new ring to node 2
scp /etc/cfsd/valuestore.ring $nodeip2:/etc/cfsd/valuestore.ring
scp /etc/cfsd/groupstore.ring $nodeip2:/etc/cfsd/groupstore.ring
ssh $nodeip2 "ln -sf /etc/cfsd/valuestore.ring /etc/cfsd/ring/valuestore.ring"
ssh $nodeip2 "ln -sf /etc/cfsd/groupstore.ring /etc/cfsd/ring/groupstore.ring"



# Create the cfsd defaults file
ssh $nodeip2 "wget -q https://raw.githubusercontent.com/getcfs/megacfs/master/nosyndinstall/etc/default/cfsd"
ssh $nodeip2 "chmod 777 cfsd"
ssh $nodeip2 "mv cfsd /etc/default/"

# Set local variables in /etc/default/cfsd
export NODENUM=2
export nodeip=$(ip addr show eth0 | grep -Po 'inet \K[\d.]+')
sed -i -e "s/NODENUM/$NODENUM/g" /etc/default/cfsd
export IDFROMVALUERING=$(ring /etc/cfsd/valuestore.ring node | grep $nodeip | awk '{ print $1 }')
sed -i -e "s/IDFROMVALUERING/$IDFROMVALUERING/g" /etc/default/cfsd
export IDFROMGROUPRING=$(ring /etc/cfsd/groupstore.ring node | grep $nodeip | awk '{ print $1 }')
sed -i -e "s/IDFROMGROUPRING/$IDFROMGROUPRING/g" /etc/default/cfsd


# Enable and Start Services
ssh $nodeip2 "systemctl daemon-reload"
ssh $nodeip2 "systemctl enable oort-valued oort-groupd formicd"
ssh $nodeip2 "systemctl restart oort-valued oort-groupd formicd"