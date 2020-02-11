# b2flow-python-tools

This tools was developer to be easy as possible to be used on b2flow project. This is are only compatible with S3Proxy.

## Install

```
pip3 install git+https://github.com/allanbatista/b2flow-python-tools
```

## Usage

```
from b2flow.python.tools.driver import Driver
from b2flow.python.tools.storage import Storage

driver = Driver("bucket_name", endpoint_url="http://endpoint-s3-proxy:7777")
storage = Storage("custom_path", driver=driver)

storage.path("teste").write(b"minha casa", "example.txt")
``` 

### Magic Handlers

Magic handlers abstract how to serialize and deserialize some kinds of data types.

```
# write dataframe
storage.handlers.pandas.write(df, "dataframe_name", batch_size=1024, compress=True)

# read dataframe
df = storage.handlers.pandas.read("dataframe_name")


storage.handlers.numpy
storage.handlers.pickle
```

## Env Variables To Configure

```
B2FLOW__PYTHON__TOOLS__BUCKET=b2flow # default bucket name
B2FLOW__PYTHON__TOOLS__HOSTNAME=http://localhost:6666 # default s3 proxy server
```