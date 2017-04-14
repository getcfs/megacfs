# Meta Information

This is brief overview on how the meta information works for CFS. Note that the behavior and data stored may change over time, but backward compatibility will be preserved if at all possible once the 1.0.0 version is released.

Whether an account exists and is active is outside the scope of CFS. This is currently handled by an external auth system, either OpenStack Keystone or a Keystone-compatible auth system.

CFS tracks the filesystems per account, some meta information about each filesystem, and the addresses granted access to each filesystem.

The following lists the group-store heirarchy where this meta information is stored.

AID = account identifier  
FSID = filesystem identifier  


## /account/<aid>/<fsid>

These are of the type formicproto.MetaAccount2Filesystem, each containing just an FSID. Use /filesystem/<fsid> to lookup additional filesystem information.


## /filesystem/<fsid>

These are of the type formicproto.MetaFilesystemEntry, each containing the AID, FSID, and filesystem name.


## /filesystem/<fsid>/address/<address>

These are of the type formicproto.MetaFilesystem2Address, each containing an IP address that is allowed access.
