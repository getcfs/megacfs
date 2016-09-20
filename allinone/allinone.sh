#!/bin/bash
set -e
export GOVERSION=1.7

echo "Using $GIT_USER as user"

echo "Setting up dev env"

apt-get update
apt-get install -y --force-yes vim git build-essential autoconf libtool libtool-bin unzip fuse mercurial
update-alternatives --set editor /usr/bin/vim.basic

# setup grpc
echo deb http://http.debian.net/debian jessie-backports main >> /etc/apt/sources.list
apt-get update
apt-get install libgrpc-dev -y --force-yes

# setup go
mkdir -p /$USER/go/bin

cd /tmp &&  wget -q https://storage.googleapis.com/golang/go$GOVERSION.linux-amd64.tar.gz
tar -C /usr/local -xzf /tmp/go$GOVERSION.linux-amd64.tar.gz
echo " " >> /$USER/.bashrc
echo "# Go stuff" >> /$USER/.bashrc
echo "export PATH=\$PATH:/usr/local/go/bin" >> /$USER/.bashrc
echo "export GOPATH=/root/go" >> /$USER/.bashrc
echo "export PATH=\$PATH:\$GOPATH/bin" >> /$USER/.bashrc
source /$USER/.bashrc

echo "Install Glide"
go get github.com/Masterminds/glide

echo "Setting up megacfs repo"
mkdir -p $GOPATH/src/github.com/getcfs
cd $GOPATH/src/github.com/getcfs/
git clone git@github.com:$GIT_USER/megacfs.git
cd megacfs
git remote add upstream git@github.com:getcfs/megacfs.git
make install

echo "Prepping /etc & /var/lib & /etc/default"
cd $GOPATH/src/github.com/getcfs/megacfs
mkdir -p /etc/syndicate/ring
mkdir -p /var/lib/oort-value/ring /var/lib/oort-value/data
mkdir -p /var/lib/oort-group/ring /var/lib/oort-group/data
mkdir -p /var/lib/formic
cp -av allinone/etc/syndicate/* /etc/syndicate
ln -s /etc/syndicate/cfssl/localhost-key.pem /var/lib/oort-value/server.key
ln -s /etc/syndicate/cfssl/localhost.pem /var/lib/oort-value/server.crt
ln -s /etc/syndicate/cfssl/ca.pem /var/lib/oort-value/ca.pem
ln -s /etc/syndicate/cfssl/localhost-key.pem /var/lib/oort-group/server.key
ln -s /etc/syndicate/cfssl/localhost.pem /var/lib/oort-group/server.crt
ln -s /etc/syndicate/cfssl/ca.pem /var/lib/oort-group/ca.pem
echo "OORT_VALUE_SYNDICATE_OVERRIDE=127.0.0.1:8443" > /etc/default/oort-valued
echo "OORT_GROUP_SYNDICATE_OVERRIDE=127.0.0.1:8444" > /etc/default/oort-groupd
ln -s /etc/syndicate/cfssl/localhost-key.pem /var/lib/formic/server.key
ln -s /etc/syndicate/cfssl/localhost.pem /var/lib/formic/server.crt
ln -s /etc/syndicate/cfssl/ca.pem /var/lib/formic/ca.pem
ln -s /etc/syndicate/cfssl/localhost-key.pem /var/lib/formic/client.key
ln -s /etc/syndicate/cfssl/localhost.pem /var/lib/formic/client.crt
ln -s $GOPATH/bin/cfs /sbin/cfs
echo -e '#!/bin/sh\ncfs mount -o $4 $1 $2 &' > /sbin/mount.cfs
chmod +x /sbin/mount.cfs

echo "Installing startup scripts"
cd $GOPATH/src/github.com/getcfs/megacfs
cp -av syndicate/packaging/root/usr/share/syndicate/systemd/synd.service /lib/systemd/system
cp -av oort/packaging/root/usr/share/oort/systemd/oort-valued.service /lib/systemd/system
cp -av oort/packaging/root/usr/share/oort/systemd/oort-groupd.service /lib/systemd/system
cp -av formic/packaging/root/usr/share/formicd/systemd/formicd.service /lib/systemd/system
systemctl daemon-reload

echo "setting up first rings with dummy nodes"
cd $GOPATH/src/github.com/getcfs/megacfs/syndicate
make ring

echo "generating custom ssl cert"
go get -u github.com/cloudflare/cfssl/cmd/...
cd /etc/syndicate/cfssl
TENDOT=$(ifconfig | awk -F "[: ]+" '/inet addr:/ { if ($4 != "127.0.0.1") print $4 }' | egrep "^10\.")
sed -e "s/TENDOTME/$TENDOT/g" /etc/syndicate/cfssl/localhost.json.tmpl | sed -e "s/HOSTNAMEME/`hostname -f`/g" > /etc/syndicate/cfssl/localhost.json
cfssl gencert -ca=/etc/syndicate/cfssl/ca.pem -ca-key=/etc/syndicate/cfssl/ca-key.pem -config=/etc/syndicate/cfssl/ca-config.json -profile=client-server /etc/syndicate/cfssl/localhost.json | cfssljson -bare localhost

echo "starting services"
systemctl start synd
systemctl start oort-valued
systemctl start oort-groupd
systemctl start formicd

echo "fixing rings"
DUMMY_NODEID=`syndicate-client search | awk '/ID/{print $2}' | head -n1`
NODEID=`syndicate-client search | awk '/ID/{print $2}' | tail -n1`
syndicate-client capacity $NODEID 1000
syndicate-client active $NODEID true
syndicate-client rm $DUMMY_NODEID
DUMMY_NODEID=`syndicate-client -addr 127.0.0.1:8444 search | awk '/ID/{print $2}' | head -n1`
NODEID=`syndicate-client -addr 127.0.0.1:8444 search | awk '/ID/{print $2}' | tail -n1`
syndicate-client -addr 127.0.0.1:8444 capacity $NODEID 1000
syndicate-client -addr 127.0.0.1:8444 active $NODEID true
syndicate-client -addr 127.0.0.1:8444 rm $DUMMY_NODEID
