- Obtain a server with at least 8GB of ram runnind debian jessie
- make sure you have fork of getcfs/megacfs
- wget https://raw.githubusercontent.com/getcfs/megacfs/master/allinone/allinone.sh
- GIT_USER=YOURGITHUBUSRENAME bash allinone.sh

- This creates:
-      account      test
-      token        test
-      filesystem   test
-      mount point  /mnt/test




- aioutil.sh will let you install a couple of tools
-      FancyVIM
-      prompt with current get branch
-      Install PROTOBUFF

BUILDPROTOBUF=yes FANCYVIM=yes FANCYPROMPT=yes bash aioutil.sh
