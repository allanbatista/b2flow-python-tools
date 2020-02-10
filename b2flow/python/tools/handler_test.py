import unittest
import pandas as pd
import numpy as np

from b2flow.python.tools.driver import Driver
from b2flow.python.tools.storage import Storage

storage = Storage('basepath/handler', driver=Driver('fromhell'))


class HandlerTestCase(unittest.TestCase):
    def test_pandas_write_and_read(self):
        storage_data = storage.path('dataframe')

        df = pd.DataFrame()
        df['name'] = "Allan", "Arley"
        df['age'] = [28, 29]
        df['weight'] = np.array([87.3, 90.56], dtype=np.float32)
        df['height'] = np.array([1.84, 1.85])

        storage_data.handlers.pandas.write(df, 'example', compress=True)
        df_result = storage_data.handlers.pandas.read('example')

        # match dtype
        self.assertEqual(df.dtypes.apply(lambda x: x.name).to_dict(), df_result.dtypes.apply(lambda x: x.name).to_dict())

        # compare all data
        self.assertEqual(df.values.tolist(), df_result.values.tolist())

    def test_numpy_write_and_read(self):
        storage_data = storage.path('numpy')

        array = np.array([
            [0.24, 0.56],
            [0.97, .1098]
        ], dtype=np.float16)

        storage_data.handlers.numpy.write(array, 'example', compress=True)
        array_result = storage_data.handlers.numpy.read('example')

        self.assertEqual(array.shape, array_result.shape)

        # compare all data
        self.assertEqual(array.tolist(), array_result.tolist())


if __name__ == '__main__':
    unittest.main()
