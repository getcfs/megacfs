- Obtain a server with at least 4GB of ram runnind debian jessie
- make sure you have forks of Syndicate/Oort/Formic (and possibly of cfs-binary-release)
- wget https://raw.githubusercontent.com/getcfs/cfs-binary-release/master/allinone/allinone.sh
- GIT_USER=YOURGHUSRENAME bash allinone.sh
- Optionally use GIT_USER=YOURGHUSERNAME BUILDPROTOBUF=yes bash allinone.sh to also build protobuf 3
- You can also use the options FANCYVIM=yes and FANCYPROMPT=yes
- STABLEDEPLOY=yes will install the binaries from cfs-binaries-release hopefully resulting in a "stable" system. You override these individually post install via something like `go install github.com/pandemicsyn/synd` 
- If using STABLEDEPLOY=yes you can also set CFSRELEASE=0.0.1 or to some other existing release tag to deploy a specific releases's binaries.


- Example of the full boat:
- GIT_USER=YOURUSERNAME BUILDPROTOBUF=yes FANCYVIM=yes FANCYPROMPT=yes STABLEDEPLOY=yes CFSRELEASE=0.0.1 bash allinone.sh
