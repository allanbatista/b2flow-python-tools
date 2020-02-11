import json
import pandas as pd
import numpy as np
import datetime
import pickle as pk


class Metadata:
    @staticmethod
    def encode(data: dict):
        return json.dumps(data)

    @staticmethod
    def decode(data: bytes):
        return json.loads(data)


class Handler:
    def __init__(self, storage):
        self.storage = storage

    def write(self, obj, filename, compress: bool = False):
        pass

    def read(self, name):
        pass


def format_value(batch):
    values = []

    for value in batch:
        if value is None:
            values.append("")
        else:
            values.append(str(value).replace('\\', '\\\\'))

    return values


def format_bool(batch):
    values = []

    for value in batch:
        if value is None:
            values.append("")
        else:
            if value:
                values.append("1")
            else:
                values.append("0")

    return values


def encode(batch, dtype):
    if str(dtype) == str(np.dtype("bool")):
        values = format_bool(batch)
    else:
        values = format_value(batch)

    return "\n".join(values).encode()


def decode(data, dtype):
    batch = data.decode().split("\n")

    if str(dtype) == str(np.dtype("bool")):
        values = []
        for value in batch:
            if value == "1":
                values.append(True)
            elif value == "0":
                values.append(False)
            else:
                values.append(None)
    else:
        values = batch

    return values


class PandasHandler(Handler):

    def write(self, df: pd.core.frame.DataFrame, name: str, batch_size=1024000, compress=False):
        """
        Persist a Pandas DataFrame in remote storage

        @param df: Pandas DataFrame
        @param name: Name will be stored
        @param batch_size: How many items per batch
        @param compress: Compress data with GZIP before save
        """
        storage = self.storage.path(name)
        storage_data = storage.path("_data")

        metadata = {
            'columns': df.columns.tolist(),
            'dtypes': df.dtypes.apply(lambda x: x.name).to_dict(),
            'count': len(df),
            'created_at': datetime.datetime.now().isoformat(),
            'compress': compress
        }

        storage.write(Metadata.encode(metadata), "_metadata")

        for column, dtype in metadata['dtypes'].items():
            values = df[column]

            count = 0
            for i in range(0, len(df), batch_size):
                count += 1
                data = encode(values[i:i+batch_size], dtype)
                storage_data.path(column).write(data, f"{str(count).zfill(10)}", compress=compress)

    def read(self, name: str):
        """
        Restore a persisted Pandas DataFrame in memory

        @param name: Name of Pandas DataFrame will be restored
        @return: pd.core.frame.DataFrame
        """
        storage = self.storage.path(name)
        storage_data = storage.path("_data")

        metadata = Metadata.decode(storage.read("_metadata"))

        data = {}
        for column, dtype in metadata['dtypes'].items():
            data[column] = []

            for entry in storage_data.path(column).list():
                data[column] += decode(entry.read(), dtype)

            data[column] = pd.Series(data[column], dtype=np.dtype(dtype))

        return pd.DataFrame(data)


class NumpyHandler(Handler):
    def write(self, arr: np.array, name: str, batch_size=1024000, compress=False):
        """
        Persist a Numpy Array in remote storage

        @param arr: Numpy Array that will be persisted
        @param name: Name will be stored
        @param batch_size: How many items per batch will be persisted
        @param compress: Compress data with GZIP before save
        """
        storage = self.storage.path(name)
        storage_data = storage.path("_data")

        metadata = {
            'shape': list(arr.shape),
            'dtype': str(arr.dtype),
            'count': len(arr),
            'created_at': datetime.datetime.now().isoformat(),
            'compress': compress
        }

        storage.write(Metadata.encode(metadata), "_metadata")

        count = 0
        for i in range(0, metadata["count"], batch_size):
            count += 1
            batch = arr[i:i+batch_size]
            storage_data.write(batch.tobytes(), f"{str(count).zfill(10)}", compress=compress)

    def read(self, name: str):
        """
        Restore a persisted Numpy Array in memory

        @param name: Name of Numpy Array will be restored
        @return: numpy.array
        """
        storage = self.storage.path(name)
        storage_data = storage.path("_data")

        metadata = Metadata.decode(storage.read("_metadata"))

        dtype = np.dtype(metadata['dtype'])
        shape = (-1, ) + tuple(metadata['shape'][1:])

        results = []
        for entry in storage_data.list():
            results.append(np.frombuffer(entry.read(), dtype=dtype).reshape(shape))

        return np.concatenate(results)


class PickleHandler(Handler):
    def write(self, obj, name: str, compress=False):
        """
        Persist a Pickle in remote storage

        @param obj: Any Object that will be persisted
        @param name: Name will be stored
        @param compress: Compress data with GZIP before save
        """
        storage = self.storage.path(name)
        storage_data = storage.path("_data")

        metadata = {
            'class': str(type(obj)),
            'created_at': datetime.datetime.now().isoformat(),
            'compress': compress
        }

        storage.write(Metadata.encode(metadata), "_metadata")
        storage_data.write(pk.dumps(obj, protocol=4), "data.pk", compress=compress)

    def read(self, name):
        """
        Restore a persisted Pickle in memory

        @param name: Name of Pickle will be restored
        @return: pickle
        """
        storage_data = self.storage.path(name).path("_data")
        return pk.loads(storage_data.read("data.pk"))


class Handlers:
    def __init__(self, storage):
        self.storage = storage
        self.pandas = PandasHandler(storage)
        self.numpy = NumpyHandler(storage)
        self.pickle = PickleHandler(storage)