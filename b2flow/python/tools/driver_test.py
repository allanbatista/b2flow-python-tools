import unittest


from b2flow.python.tools.driver import Driver


class DriverTestCase(unittest.TestCase):
    def test_write_and_read(self):
        driver = Driver('fromhell')
        driver.write(b"oi", "path/to/oi.txt")
        self.assertEqual(driver.read("path/to/oi.txt"), b"oi")


if __name__ == '__main__':
    unittest.main()
