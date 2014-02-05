"""
The redis protocol only knows about bytes, but we like to have strings inside
Python. This file contains some helper classes for decoding the bytes to
strings and encoding the other way around. We also have a `BytesEncoder`, which
provides raw access to the redis server.
"""

__all__ = (
        'BaseEncoder',
        'BytesEncoder',
        'UTF8Encoder',
)

class BaseEncoder:
    """
    Abstract base class for all encoders.
    """
    #: The native Python type from which we encode, or to which we decode.
    native_type = None

    def encode_from_native(self, data):
        """
        Encodes the native Python type to network bytes.
        Usually this will encode a string object to bytes using the UTF-8
        encoding. You can either override this function, or set the
        `encoding` attribute.
        """
        raise NotImplementedError

    def decode_to_native(self, data):
        """
        Decodes network bytes to a Python native type.
        It should always be the reverse operation of `encode_from_native`.
        """
        raise NotImplementedError


class BytesEncoder(BaseEncoder):
    """
    For raw access to the Redis database.
    """
    #: The native Python type from which we encode, or to which we decode.
    native_type = bytes

    def encode_from_native(self, data):
        return data

    def decode_to_native(self, data):
        return data


class StringEncoder(BaseEncoder):
    """
    Abstract base class for all string encoding encoders.
    """
    #: Redis keeps all values in binary. Set the encoding to be used to
    #: decode/encode Python string values from and to binary.
    encoding = None

    #: The native Python type from which we encode, or to which we decode.
    native_type = str

    def encode_from_native(self, data):
        """ string to bytes """
        return data.encode(self.encoding)

    def decode_to_native(self, data):
        """ bytes to string """
        return data.decode(self.encoding)


class UTF8Encoder(StringEncoder):
    """
    Encode strings to and from utf-8 bytes.
    """
    encoding = 'utf-8'
