# Tuple package

There are 2 kinds of tuples:

1. FoundationDB style tuples (`Tuple`)
2. Hierarchical tuples (`HierarchitcalTuple`)

The difference is in the encoding logic, which changes the ordering of tuples:

This allows you to build more traditional filesystem-like structures.

### Tuple

These are lexicographically ordered:

```
dir/
dir/a
dir/a/1
dir/b
```

The packages have fundamentally interfaces so they are less likely to be accidentally mixed.

For example, this ordering is the same as S3 during a ListObject call.

### HierarchicalTuple

Order first by hierarchy, then by lexicographical order. Only ASCII characters are supported!

Something with fewer entries will always come before something with more entries.

This is more akin to how you might `ls` a filesystem, where entries are ordered by their hierarchy, then lexicographical order.

You can range scan a specific prefix `dir` via `[0xffdir, 0xffdir0xff)`

```
dir/
dir/a
dir/b
dir/a/1
```

The `.RangeKeys()` method can be used to generate the keys that should be passed to range scan functions to ensure you only get direct children. With the above example `HierarchicalTuple{[]byte("dir").RangeKeys()` would result in a scan finding `dir/1` and `dir/b` (/ used as visual separator), but notably not `dir` or `dir/a/1`.