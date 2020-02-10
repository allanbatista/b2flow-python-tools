import json, io
import pandas as pd
import numpy as np
import datetime
import csv
import pickle as pk


class Metadata:
    @staticmethod
    def encode(data: dict):
        return json.dumps(data).encode()

    @staticmethod
    def decode(data: bytes):
        return json.loads(data)


class Handler:
    def __init__(self, storage):
        self.storage = storage

    def encoder(self, obj):
        pass

    def decoder(self, data: io.BytesIO, metadata: dict):
        pass

    def write(self, obj, filename, compress: bool = False):
        data, metadata = self.encoder(obj)

        self.storage.write(Metadata.encode(metadata), f"{filename}.metadata")
        self.storage.write(data, filename, compress)

    def read(self, filename):
        return self.decoder(self.storage.read(filename, as_memory_io=True),
                            Metadata.decode(self.storage.read(f"{filename}.metadata")))


class PandasHandler(Handler):
    def encoder(self, df: pd.core.frame.DataFrame):
        data = df.to_csv(quoting=csv.QUOTE_NONNUMERIC).encode()
        metadata = {
            'columns': df.columns.tolist(),
            'dtypes': df.dtypes.apply(lambda x: x.name).to_dict(),
            'count': len(df),
            'length': len(data),
            'created_at': datetime.datetime.now().isoformat()
        }
        return data, metadata

    def decoder(self, data: io.BytesIO, metadata: dict):
        return pd.read_csv(data, dtype=metadata['dtypes'], index_col=0, header=0)


class NumpyHandler(Handler):
    def encoder(self, array: np.ndarray):
        data = array.tobytes()
        metadata = {
            'shape': array.shape,
            'dtype': array.dtype.name,
            'count': len(array),
            'length': len(data),
            'created_at': datetime.datetime.now().isoformat()
        }
        return data, metadata

    def decoder(self, data: io.BytesIO, metadata: dict):
        return np.frombuffer(data.read(), dtype=metadata['dtype']).reshape(metadata['shape'])


class PickleHandler(Handler):
    def encoder(self, obj):
        data = pk.dumps(obj, protocol=4)
        metadata = {
            'class': str(type(obj)),
            'length': len(data),
            'created_at': datetime.datetime.now().isoformat()
        }
        return data, metadata

    def decoder(self, data: io.BytesIO, metadata: dict):
        return pk.loads(data.read())


class Handlers:
    def __init__(self, storage):
        self.storage = storage
        self.pandas = PandasHandler(storage)
        self.numpy = NumpyHandler(storage)
        self.pickle = PickleHandler(storage)