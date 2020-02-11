import json, io
import pandas as pd
import numpy as np
import datetime


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

    def write(self, obj, filename, compress: bool = False):
        pass

    def read(self, filename):
        pass


class PandasHandler(Handler):
    def format_bool(self, batch):
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

    def format_value(self, batch):
        values = []

        for value in batch:
            if value is None:
                values.append("")
            else:
                values.append(str(value).replace('\\', '\\\\'))

        return values

    def encode(self, batch, dtype):
        if str(dtype) == str(np.dtype("bool")):
            values = self.format_bool(batch)
        else:
            values = self.format_value(batch)

        return "\n".join(values).encode()

    def decode(self, bytes, dtype):
        batch = bytes.decode().split("\n")

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

    def write(self, df: pd.core.frame.DataFrame, name: str, batch_size=1024000, compress=True):
        storage = self.storage.path(name)

        metadata = {
            'columns': df.columns.tolist(),
            'dtypes': df.dtypes.apply(lambda x: x.name).to_dict(),
            'count': len(df),
            'created_at': datetime.datetime.now().isoformat(),
            'compress': compress
        }

        storage.write(Metadata.encode(metadata), f"_metadata")

        for column, dtype in metadata['dtypes'].items():
            values = df[column]

            count = 0
            for i in range(0, len(df), batch_size):
                count += 1
                data = self.encode(values[i:i+batch_size], dtype)
                storage.path(column).write(data, f"{str(count).zfill(10)}", compress=True)

    def read(self, name: str):
        storage = self.storage.path(name)
        metadata = Metadata.decode(storage.read("_metadata"))

        data = {}
        for column, dtype in metadata['dtypes'].items():
            data[column] = []

            for entry in storage.path(column).list():
                data[column] += self.decode(entry.read(), dtype)

            data[column] = pd.Series(data[column], dtype=np.dtype(dtype))

        return pd.DataFrame(data)


class DirectoryHandler:
    def __init__(self, storage):
        self.storage = storage

    def upload(self, remote_storage):
        pass

    def download(self, remote_storage):
        pass


class Handlers:
    def __init__(self, storage):
        self.storage = storage
        self.pandas = PandasHandler(storage)
        # self.numpy = NumpyHandler(storage)
        # self.pickle = PickleHandler(storage)