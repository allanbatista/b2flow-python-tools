import unittest, os

from b2flow.python.tools.config import get


class ConfigTestCase(unittest.TestCase):
    def test_config(self):
        self.assertEqual(get('B2FLOW__PYTHON__TOOLS__BUCKET'), "b2flow")
        self.assertEqual(get('B2FLOW__PYTHON__TOOLS__HOSTNAME'), "localhost:6666")

    def test_should_update_configs(self):
        os.environ['B2FLOW__PYTHON__TOOLS__BUCKET'] = "fromhell"
        os.environ['B2FLOW__PYTHON__TOOLS__HOSTNAME'] = "localhost:8888"

        self.assertEqual(get('B2FLOW__PYTHON__TOOLS__BUCKET'), "fromhell")
        self.assertEqual(get('B2FLOW__PYTHON__TOOLS__HOSTNAME'), "localhost:8888")


if __name__ == '__main__':
    unittest.main()
