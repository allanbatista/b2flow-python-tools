import unittest

from b2flow.python.tools.driver import Driver
from b2flow.python.tools.storage import Storage


class StorageTestCase(unittest.TestCase):
    def test_should_join(self):
        storage = Storage('/basepath')

        self.assertEqual(storage.join("second", "third"), "basepath/second/third")

    def test_should_create_path(self):
        storage = Storage('basepath')

        self.assertEqual(storage.directory, "basepath")
        self.assertEqual(storage.path("second").directory, "basepath/second")

    def test_should_go_to_parent_path(self):
        storage = Storage('basepath/second')

        self.assertEqual(storage.parent.directory, "basepath")

    def test_should_write_and_read_bytes(self):
        storage = Storage('basepath/second', driver=Driver('fromhell'))

        storage.write(b"test", "example.txt")
        data = storage.read("example.txt")
        self.assertEqual(b"test", data)

    def test_should_write_and_read_bytes_with_compress(self):
        storage = Storage('basepath/second', driver=Driver('fromhell'))

        storage.write(b"test", "example.txt", compress=True)
        data = storage.read("example.txt")
        self.assertEqual(b"test", data)


if __name__ == '__main__':
    unittest.main()
