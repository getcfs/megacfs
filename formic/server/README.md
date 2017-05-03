# Meta Information

This is a brief overview on how the meta information works for CFS. Note that the behavior and data stored may change over time, but backward compatibility will be preserved if at all possible once the 1.0.0 version is released.

Whether an account exists and is active is outside the scope of CFS. This is currently handled by an external auth system, either OpenStack Keystone or a Keystone-compatible auth system.

CFS tracks the filesystems per account, some meta information about each filesystem, and the addresses granted access to each filesystem.

AID = account identifier  
FSID = filesystem identifier  

The following lists the group-store heirarchy where this meta information is stored:


## `/account/<aid>/<fsid>`

These are of the type formicproto.MetaAccount2Filesystem, each containing just an FSID. Use `/filesystem/<fsid>` to lookup additional filesystem information.


## `/filesystem/<fsid>`

These are of the type formicproto.MetaFilesystemEntry, each containing the AID, FSID, and filesystem name.


## `/filesystem/<fsid>/address/<address>`

These are of the type formicproto.MetaFilesystem2Address, each containing an IP address that is allowed access.


# File System Data

Each file has an inode number, like with most filesystems.

Inode number 1 is the root inode for the entire filesystem, the root directory, /.

The first block of a file or directory, block 0, is the root block containing additional information about the file or directory. This information contains items such as mtime, ctime, mode, uid, gid, xattrs, etc.

Any remaining blocks are data blocks for a file, each with a corresponding crc value. So, the first data block is block 1.

The store keypairs are generated using the combination of fsid, inode number, and block number.

Everything stored in the value store is a formicproto.FileBlock, which contains a version and checksum in addition to the actual data. The actual data may be file contents for blocks 1 onward, or a formicproto.INodeEntry for block 0.

Everything stored in the group store is a formicproto.DirEntry except as noted in the Meta Information section.

