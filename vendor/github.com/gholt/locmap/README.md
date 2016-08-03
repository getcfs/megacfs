# ValueLocMap
## Development Repository

**Experimental: No stable version of this package yet exists; it is still in
early development.**

Package locmap provides a concurrency-safe data structure that maps keys to
value locations. A key is 128 bits and is specified using two uint64s (keyA,
keyB). A value location is specified using a blockID, offset, and length
triplet. Each mapping is assigned a timestamp and the greatest timestamp wins.

The timestamp usually has some number of the lowest bits in use for state
information such as active and inactive entries. For example, the lowest bit
might be used as 0 = active, 1 = deletion marker so that deletion events are
retained for some time period before being completely removed with Discard.
Exactly how many bits are used and what they're used for is outside the
scope of the mapping itself.

This implementation essentially uses a tree structure of slices of key to
location assignments. When a slice fills up, an additional slice is created
and half the data is moved to the new slice and the tree structure grows. If
a slice empties, it is merged with its pair in the tree structure and the
tree shrinks. The tree is balanced by high bits of the key, and locations
are distributed in the slices by the low bits.

There is also a modified form of the data structure called GroupLocMap that
expands the primary key of the map to two 128 bit keys and offers a GetGroup
method which retrieves all matching items for the first key.

[API Documentation](http://godoc.org/github.com/gholt/locmap)  
[Why Templating?](WHY_TEMPLATING.md)

This is the latest development area for the package.  
Eventually a stable version of the package will be established but, for now,
all things about this package are subject to change.

> Copyright See AUTHORS. All rights reserved.  
> Use of this source code is governed by a BSD-style  
> license that can be found in the LICENSE file.
