#!/bin/bash
set -e
export GOVERSION=1.6

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
mkdir -p $GOPATH/src/github.com/Masterminds
cd $GOPATH/src/github.com/Masterminds
git clone git@github.com:Masterminds/glide
cd glide
make install

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
echo -e '#!/bin/sh\ncfs mount $1 $2 -o $4 > /dev/null &' > /sbin/mount.cfs
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

echo
echo "!! Don't forget to remove the place holder nodes from the ring once you've started your nodes"
echo
echo "To start services run:"
echo "systemctl start synd"
echo "systemctl start oort-valued"
echo "systemctl start oort-groupd"
echo "systemctl start formicd"
echo
echo "Create a mount directory"
echo "mkdir -p /mnt/cfs"
echo
echo "Use acctdv2 and oort-cli to create an account"
echo
echo "You can create an file system with the cfs client"
echo "Example:"
echo "cfs -T [account token uuid] create aio://[account uuid] -N test"
echo
echo "Once you create a file system you need to grant a local ip address 127.0.0.1"
echo "Example:"
echo "cfs -T [account token uuid] grant aio://[account uuid]/[file system uuid] -addr 127.0.0.1"
echo
echo "For example: to create a cfsfuse mount point create the location and run the mount command:"
echo "mount -t cfs  aio0://cfsteam/allinone/ /mnt/fsdrive -o host=localhost:8445"
