import boto3, botocore

from b2flow.python.tools.config import get, logger


class Driver:
    def __init__(self, bucket_name: str = get('B2FLOW__PYTHON__TOOLS__BUCKET'),
                 endpoint_url: str = get('B2FLOW__PYTHON__TOOLS__HOSTNAME')):

        self.bucket_name = bucket_name
        session = boto3.session.Session(aws_access_key_id='identity', aws_secret_access_key='credential')
        config = boto3.session.Config(s3={'addressing_style': 'path'})
        self.client = session.client('s3', endpoint_url=endpoint_url, config=config)
        self.resource = session.resource('s3', endpoint_url=endpoint_url, config=config)

        try:
            self.client.create_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError:
            pass

    def write(self, body: bytes, filepath: str):
        self.client.put_object(Bucket=self.bucket_name, Body=body, Key=filepath)
        logger.info("write", self.bucket_name, filepath, f"{len(body)}bytes")

    def read(self, filepath: str):
        obj = self.client.get_object(Bucket=self.bucket_name, Key=filepath)
        body = obj['Body'].read()
        logger.info("read", self.bucket_name, filepath, f"{len(body)}bytes")
        return body

    def list(self, prefix: str = ""):
        objects = self.client.list_objects(Bucket=self.bucket_name, Prefix=prefix).get("Contents") or []
        logger.info("list", self.bucket_name, prefix, f"{len(objects)} objects")
        return objects
