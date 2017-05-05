# Creating a Development Environment

```
# Create an Ubuntu 14.04 server. Keystone set up is differently with newer
# versions distributed with newer versions of Ubuntu and we couldn't really
# figure it out quickly, so we stuck with Ubuntu 14.04 for now.

# Become root.
# At this time, development is just a whole lot easier as the root user.

# Get the system up to date and prequisite packages installed:
apt-get update
apt-get upgrade
apt-get install autoconf g++ git keystone libtool make unzip

# Get the newest version of Go from https://golang.org/dl/
# In this case, we're using:
# https://storage.googleapis.com/golang/go1.8.1.linux-amd64.tar.gz
cd ~
wget https://storage.googleapis.com/golang/go1.8.1.linux-amd64.tar.gz
tar zxf go1.8.1.linux-amd64.tar.gz
echo 'export PATH=$PATH:~/go/bin:~/bin' >> ~/.bashrc
echo 'export GOROOT=~/go' >> ~/.bashrc
echo 'export GOPATH=~' >> ~/.bashrc
source ~/.bashrc

# Set up the OpenStack Keystone auth system:
export OS_SERVICE_TOKEN=ADMIN
export OS_SERVICE_ENDPOINT=http://127.0.0.1:35357/v2.0
keystone tenant-create --name admin --description "Admin Tenant"
keystone role-create --name admin
keystone user-create --name admin --pass admin --tenant admin
keystone user-role-add --user admin --role admin --tenant admin 
keystone tenant-create --name service --description "Service Tenant"
keystone role-create --name service
keystone user-create --name service --pass service --tenant service
keystone user-role-add --user service --role service --tenant service
keystone tenant-create --name test --description "Test Tenant"
keystone user-create --name test --pass test --tenant test
keystone user-create --name cfs --pass cfs --tenant service
keystone user-role-add --user cfs --role admin --tenant service
keystone service-create --name cfs --type cfs --description cfs
keystone endpoint-create --publicurl 1.1.1.1:8445 \
    --service-id $(keystone service-list | awk '/ cfs / {print $2}')

# Install, configure, and run CFS from source:
go get github.com/getcfs/megacfs # You'll get a build error; that's fine.
cd ~/src/github.com/getcfs/megacfs
make install
cd ~
cfsadm init
cfsadm add 127.0.0.1
cfsd &
echo http://localhost:5000/ admin admin 127.0.0.1 | cfs configure
cfs create devfs | cut -d ' ' -f2 > ~/.cfs.fsid
cfs grant `cat ~/.cfs.fsid`
mkdir ~/mnt
cfs mount `cat ~/.cfs.fsid` ~/mnt &

# You should now be able to use ~/mnt as a regular file system:
df -h
echo Testing > ~/mnt/testfile.txt
cat ~/mnt/testfile.txt
ls -la ~/mnt

# If you need to work on the protobuf/grpc parts of the code, you'll need to
# install Google ProtoBuf from source:
git clone https://github.com/google/protobuf.git
cd protobuf
./autogen.sh
./configure
make # This takes a long time.
make install
ldconfig
cd ~
go get github.com/golang/protobuf/...
# This last part tests that your protobuf set up is correct:
cd ~/src/github.com/getcfs/megacfs/formic/formicproto
./regen.sh
cd ~
```
