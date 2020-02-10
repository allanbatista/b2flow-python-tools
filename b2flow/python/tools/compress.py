import io, binascii
import gzip as libgzip


def is_gz_file(data):
    """
    check if bytes are gzipped
    """
    return binascii.hexlify(data[:2]) == b'1f8b'


def gzip(raw):
    """
    compress data into gzip

    byte[] uncompressed

    return byte[]
    """

    bytes_io = io.BytesIO()
    with libgzip.open(bytes_io, 'wb') as f:
        f.write(raw)

    bytes_io.seek(0)
    return bytes_io.read()


def ungzip(gzipped):
    """
    uncompress gzip bytes

    byte[]

    return byte[]
    """
    if is_gz_file(gzipped):
        bytes_io = io.BytesIO(gzipped)
        bytes_io.seek(0)
        with libgzip.open(bytes_io, "rb") as f:
            return f.read()

    return gzipped
