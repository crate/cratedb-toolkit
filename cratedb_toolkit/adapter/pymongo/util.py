import binascii
import struct
import time
from typing import Optional, Union

from bson import ObjectId

_MAX_COUNTER_VALUE = 0xFFFFFF


class AmendedObjectId:
    def __init__(self, oid: Optional[Union[str, ObjectId, bytes]] = None) -> None:
        """
        Initialize a new (almost) MongoDB-compatible ObjectId.

        TODO: Make it fully compatible, for example by reducing width.
        """
        self.__id = None
        if oid is None:
            self.__generate()
        else:
            self.__id = oid

    @classmethod
    def from_str(cls, oid: str):
        return cls(bytes(oid, "ascii"))

    def __str__(self) -> str:
        return binascii.hexlify(self.__id).decode()  # type: ignore[arg-type]

    def __repr__(self) -> str:
        return f"ObjectId('{self!s}')"

    def __generate(self) -> None:
        """
        Generate a new value for this ObjectId.

        TODO: Generate IDs of the same width like CrateDB.
        """
        # 4 bytes current time
        oid = struct.pack(">I", int(time.time()))

        # 5 bytes random
        oid += ObjectId._random()

        # 3 bytes inc
        with ObjectId._inc_lock:
            oid += struct.pack(">I", ObjectId._inc)[1:4]
            ObjectId._inc = (ObjectId._inc + 1) % (_MAX_COUNTER_VALUE + 1)

        self.__id = oid
