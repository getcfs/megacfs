# CFS Getting Started Guide

[Installing FUSE](#installing-fuse)

[Installing the CFS Client](#installing-the-cfs-client)

[Configuring the CFS Client](#configuring-the-cfs-client)

[Creating a Filesystem](#creating-a-filesystem)

[Granting Access to a Filesystem](#granting-access-to-a-filesystem)

[Mounting a Filesystem](#mounting-a-filesystem)

[Unmounting a Filesystem](#unmounting-a-filesystem)

[Revoking Access to a Filesystem](#revoking-access-to-a-filesystem)

[Deleting a Filesystem](#deleting-a-filesystem)

# Installing FUSE

CFS uses FUSE to send/receive file operations to/from the Linux kernel. Run one of the following commands to install FUSE.
```
sudo apt-get install fuse   # Debian or Ubuntu
sudo yum install fuse       # CentOS
sudo dnf install fuse       # Fedora
```

**Note:** You might need to update your list of packages before installing FUSE.

CFS requires FUSE version 2.6 or greater, which you can verify with the following command.
```
$ fusermount -V
fusermount version: 2.9.3
```

# Installing the CFS Client

The CFS client is used to manage your CFS filesystems in addition to handling client/server communication for each mounted CFS filesystem.

The following command will install/update the CFS client.
```
curl -fsSL https://raw.githubusercontent.com/getcfs/megacfs/master/install | sudo bash
```

Try running ```cfs``` to verify the client is installed.
```
$ cfs
Usage:
    cfs <command> [options] <args>
Commands:
    configure    create configuration file
    list         list all filesystems
    create       create a new filesytem
    show         show filesystem details
    update       update filesystem name
    delete       delete an existing filesystem
    grant        grant access to a filesystem
    revoke       revoke access to a filesystem
    mount        mount an existing filesystem
    version      show client version
    help         show usage for cfs
Examples:
    cfs configure
    cfs create <name>
    cfs grant <ip> <fsid>
    cfs mount <region>:<fsid> <mountpoint>
```

You can also verify your client version with the following command.
```
$ cfs version
version: 0.1.0
commit: 2f31fde
build date: 2016-08-24.18:05:42
go version: go-version-go1.7-linux/amd64
```

# Configuring the CFS Client

Before you can use the CFS client you need to specify the region, username and API key to use when managing your CFS filesystems.
CFS will store these creditials in a file called ```~/.cfs.json``` so you do not need to specify them for every command.

Run the following command to configure the CFS client. 
```
cfs configure
```

Here is an example of the interactive session.
```
$ cfs configure
This is an interactive session to configure the cfs client.
CFS Region: iad
CFS Username: myusername
CFS APIKey: 178a937c929ef0976c058258a62353be
```

**Note:** CFS is currently only available in the IAD region.

In some cases it might be easier to provide the region, username and API key via stdin.
```
echo "<region> <username> <apikey>" | cfs configure
```

# Creating a Filesystem

Now that your client is configured you can create your first CFS filesystem using the "create" command.
```
cfs create <name>
```

Here we create a filesystem called "myfs".
```
$ cfs create myfs
ID: df9ce453-44bf-453f-8d78-591f79ae1401
```

**Note:** Filesystem names can include spaces but you must use double quotes around the name.

# Granting Access to a Filesystem

CFS uses IP based auth and TLS to ensure only clients with access granted can mount and use a CFS filesystem.
Before any client can mount a CFS filesystem you must first grant access to that client's ServiceNet IP address (10.x.x.x) using the "grant" command.

```
cfs grant <ip> <fsid>
```

Here we grant 10.1.2.3 access to the "myfs" filesystem we created in the previous section. 
```
$ cfs grant 10.1.2.3 df9ce453-44bf-453f-8d78-591f79ae1401
```

The "show" command will display the IP addresses that are allowed to connect to a CFS filesystem.
```
cfs show <fsid>
```
Here we validate 10.1.2.3 was granted access to our filesystem.
```
$ cfs show df9ce453-44bf-453f-8d78-591f79ae1401
ID: df9ce453-44bf-453f-8d78-591f79ae1401
Name: myfs
IP: 10.1.2.3
```

**Note:** You can use the following commands to find your ServiceNet IP address (10.x.x.x) and CFS filesystem ID.
```
ip addr | grep -o 'inet 10.[0-9.]*'     # (or ifconfig) to find the ServiceNet IP (10.x.x.x)
cfs list                                # to get the CFS filesystem ID
```

# Mounting a Filesystem

Now that you have created a CFS filesystem and granted access to your CFS client you can mount the filesystem.

First create a mountpoint for your filesystem, then use the "mount" command to mount your filesystem.
```
cfs mount -o <options> <region>:<fsid> <mountpoint>
```
Here we create a mountpoint and mount our filesystem.
```
$ mkdir myfs
$ cfs mount iad:df9ce453-44bf-453f-8d78-591f79ae1401 myfs &
```
**Note:** The "mount" command runs in the foreground so you will need to background it with "&" or open another terminal session to use the filesystem.

By default a CFS filesystem will only allow access to the user that mounted the filesystem.
To allow other users on the host to access the filesystem you must use the allow_other mount option.
```
$ sudo mkdir -p /mnt/myfs
$ sudo cfs mount -o allow_other iad:df9ce453-44bf-453f-8d78-591f79ae1401 /mnt/myfs &
```

To persist the mount across reboots you will need to add the filesystem to the ```/etc/fstab``` file with the following format.
```
<region>:<fsid> <mountpoint> cfs allow_other,_netdev 0 0
```
**Note:** The _netdev option is required so the system does not attempt to mount the filesystem before the network is up.

Here we add our filesystem to ```/etc/fstab```.
```
$ sudo sh -c "echo 'iad:df9ce453-44bf-453f-8d78-591f79ae1401 /mnt/myfs cfs allow_other,_netdev 0 0' >> /etc/fstab"
```

Once your filesystem is added to ```/etc/fstab``` you can use the standard Linux mount command to mount your filesystem using only the mountpoint.
```
$ sudo mount /mnt/myfs
```

**Note:** You can use the debug mount option to get debug information for all filesystem operations.
```
$ sudo mount -o debug /mnt/myfs
```

**Note:** The "cfs mount" command does not require the CFS client to be configured and thus have access to the CFS management commands.
Mount only requires a region and CFS filesystem ID because it uses IP based auth.
If the client IP was granted access it will be able to mount the filesystem.
This allows for greater security on the client.
In addition, filesystem access can be granted to any client without providing access to the CFS management capabilities.

# Unmounting a Filesystem

The standard Linux "umount" command will unmount a CFS filesystem.
```
$ sudo umount /mnt/myfs
```
**Note:** In certain failure scenarios the Linux kernel will not cleanly unmount the filesystem.
In this case you can force a lazy unmount with the following command.
```
$ sudo fusermount -uz /mnt/myfs
```

# Revoking Access to a Filesystem

You can use the "revoke" command to revoke access to a CFS filesystem.
```
cfs revoke <ip> <fsid>
```

Here we revoke access to our filesystem from the 10.1.2.3 client.
```
$ cfs revoke 10.1.2.3 df9ce453-44bf-453f-8d78-591f79ae1401
```

You can use the "show" command to verify which clients have access.
```
cfs show <fsid>
```

Here we verify that the 10.1.2.3 client no longer has access to our filesystem.
```
$ cfs show df9ce453-44bf-453f-8d78-591f79ae1401
ID: df9ce453-44bf-453f-8d78-591f79ae1401
Name: myfs
```

**Note:** Revoking access does not terminate existing mounts from the client IP.
It only prevents any additional mounts from the client IP.

# Deleting a Filesystem

If a filesystem is empty you can remove it using the "delete" command.
```
cfs delete <fsid>
```

Here we delete the filesystem we created during this guide.
```
$ cfs delete df9ce453-44bf-453f-8d78-591f79ae1401
```

You can list your CFS filesystems to verify the delete was successful.
```
cfs list
```