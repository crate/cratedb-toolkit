"""
OrderedDictX by Zuzu Corneliu.

For the keeping of order case (the other one is trivial, remove old and add new
one): I was not satisfied with the ordered-dictionary needing reconstruction
(at least partially), obviously for efficiency reasons, so I've put together a
class (OrderedDictX) that extends OrderedDict and allows you to do key changes
efficiently, i.e. in O(1) complexity. The implementation can also be adjusted
for the now-ordered built-in dict class.

It uses 2 extra dictionaries to remap the changed keys ("external" - i.e. as
they appear externally to the user) to the ones in the underlying OrderedDict
("internal") - the dictionaries will only hold keys that were changed so as
long as no key changing is done they will be empty.

As expected, the splicing method is extremely slow (didn't expect it to be that
much slower either though) and uses a lot of memory, and the O(N) solution of
@Ashwini Chaudhary (bug-fixed though, del also needed) is also slower, 17X
times in this example.

Of course, this solution being O(1), compared to the O(N) OrderedDictRaymond
the time difference becomes much more apparent as the dictionary size
increases, e.g. for 5 times more elements (100000), the O(N) is 100X slower.

https://stackoverflow.com/questions/16475384/rename-a-dictionary-key/75115645#75115645
"""

from collections import OrderedDict


class OrderedDictX(OrderedDict):
    def __init__(self, *args, **kwargs):
        # Mappings from new->old (ext2int), old->new (int2ext).
        # Only the keys that are changed (internal key doesn't match what the user sees) are contained.
        self._keys_ext2int = OrderedDict()
        self._keys_int2ext = OrderedDict()
        self.update(*args, **kwargs)

    def rename_key(self, k_old, k_new):
        # Validate that the old key is part of the dict
        if not self.__contains__(k_old):
            raise KeyError(f"Cannot rename key {k_old} to {k_new}: {k_old} not existing in dict")

        # Return if no changing is actually to be done
        if len(OrderedDict.fromkeys([k_old, k_new])) == 1:
            return

        # Validate that the new key would not conflict with another one
        if self.__contains__(k_new):
            raise KeyError(f"Cannot rename key {k_old} to {k_new}: {k_new} already in dict")

        # Change the key using internal dicts mechanism
        if k_old in self._keys_ext2int:
            # Revert change temporarily
            k_old_int = self._keys_ext2int[k_old]
            del self._keys_ext2int[k_old]
            k_old = k_old_int
            # Check if new key matches the internal key
            if len(OrderedDict.fromkeys([k_old, k_new])) == 1:
                del self._keys_int2ext[k_old]
                return

        # Finalize key change
        self._keys_ext2int[k_new] = k_old
        self._keys_int2ext[k_old] = k_new

    def __contains__(self, k) -> bool:
        if k in self._keys_ext2int:
            return True
        if not super().__contains__(k):
            return False
        return k not in self._keys_int2ext

    def __getitem__(self, k):
        if not self.__contains__(k):
            # Intentionally raise KeyError in ext2int
            return self._keys_ext2int[k]
        return super().__getitem__(self._keys_ext2int.get(k, k))

    def __setitem__(self, k, v):
        if k in self._keys_ext2int:
            return super().__setitem__(self._keys_ext2int[k], v)
        # If the key exists in the internal state but was renamed to a k_ext,
        # employ this trick: make it such that it appears as if k_ext has also been renamed to k
        if k in self._keys_int2ext:
            k_ext = self._keys_int2ext[k]
            self._keys_ext2int[k] = k_ext
            k = k_ext
        return super().__setitem__(k, v)

    def __delitem__(self, k):
        if not self.__contains__(k):
            # Intentionally raise KeyError in ext2int
            del self._keys_ext2int[k]
        if k in self._keys_ext2int:
            k_int = self._keys_ext2int[k]
            del self._keys_ext2int[k]
            del self._keys_int2ext[k_int]
            k = k_int
        return super().__delitem__(k)

    def __iter__(self):
        yield from self.keys()

    def __reversed__(self):
        for k in reversed(super().keys()):
            yield self._keys_int2ext.get(k, k)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, dict):
            return False
        if len(self) != len(other):
            return False
        for (k, v), (k_other, v_other) in zip(self.items(), other.items()):
            if k != k_other or v != v_other:
                return False
        return True

    def update(self, *args, **kwargs):
        for k, v in OrderedDict(*args, **kwargs).items():
            self.__setitem__(k, v)

    def popitem(self, last=True) -> tuple:
        if not last:
            k = next(iter(self.keys()))
        else:
            k = next(iter(reversed(self.keys())))
        v = self.__getitem__(k)
        self.__delitem__(k)
        return k, v

    class OrderedDictXKeysView:
        def __init__(self, odx: "OrderedDictX", orig_keys):
            self._odx = odx
            self._orig_keys = orig_keys

        def __iter__(self):
            for k in self._orig_keys:
                yield self._odx._keys_int2ext.get(k, k)

        def __reversed__(self):
            for k in reversed(self._orig_keys):
                yield self._odx._keys_int2ext.get(k, k)

    class OrderedDictXItemsView:
        def __init__(self, odx: "OrderedDictX", orig_items):
            self._odx = odx
            self._orig_items = orig_items

        def __iter__(self):
            for k, v in self._orig_items:
                yield self._odx._keys_int2ext.get(k, k), v

        def __reversed__(self):
            for k, v in reversed(self._orig_items):
                yield self._odx._keys_int2ext.get(k, k), v

    def keys(self):
        return self.OrderedDictXKeysView(self, super().keys())

    def items(self):
        return self.OrderedDictXItemsView(self, super().items())

    def copy(self):
        return OrderedDictX(self.items())
