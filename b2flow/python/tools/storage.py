import os, io, re

from b2flow.python.tools.handler import Handlers
from b2flow.python.tools.driver import Driver
from b2flow.python.tools.compress import gzip, ungzip


def remove_first_back_slash(path: str):
    if path.startswith('/'):
        return path[1:]
    else:
        return path


def to_in_memory_io(data: bytes):
    in_memory_io = io.BytesIO(data)
    in_memory_io.seek(0)
    return in_memory_io


class Storage:
    """
    This class abstract storage layer
    """
    def __init__(self, directory: str, driver: Driver = None):
        self.directory = remove_first_back_slash(directory)
        self.handlers = Handlers(self)
        self.driver = driver

    def path(self, directory: str):
        """
        create a directory scope

        string path

        return Storage
        """
        return Storage(self.join(directory), driver=self.driver)

    @property
    def parent(self):
        """
        go to parent path

        return Storage
        """
        parent_path = "/".join(self.directory.split("/")[:-1])
        return Storage(parent_path, driver=self.driver)

    def join(self, *args):
        """
        join a filepath with current path of storage

        string filepath1, filepathN, ...

        return string
        """
        return os.path.join(self.directory, *args)

    def write(self, data: bytes, name: str, compress: bool = False):
        """
        write bytes to a remote file

        byte[] data
        string filename
        boolean compress=False

        return None
        """
        filepath = self.join(name)
        if compress:
            self.driver.write(gzip(data), filepath)
        else:
            self.driver.write(data, filepath)

    def read(self, filename: str, as_memory_io: bool = False):
        """
        read remote file as bytesentiry

        string filename

        return byte[]
        """
        data = ungzip(self.driver.read(self.join(filename)))

        if as_memory_io:
            data = to_in_memory_io(data)

        return data

    def list(self):
        """
        should list all files in a current prefix path

        return Entry[]
        """
        entries = []
        regex = re.compile('^%s' % self.directory)

        for obj in self.driver.list(self.directory):
            path, name = os.path.split(obj['Key'])
            entries.append(Entry(self.path(regex.sub("", path)), name))

        return entries


class Entry:
    def __init__(self, storage: Storage, name: str):
        self.name = name
        self.storage = storage

    def read(self):
        return self.storage.read(self.name)

    def write(self, data: bytes, compress: bool = False):
        self.storage.write(data, self.name, compress)