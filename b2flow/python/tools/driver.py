import boto3, botocore


class Driver:
    def __init__(self, bucket_name: str, endpoint_url: str = 'http://localhost:6666'):
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
        return self.client.put_object(Bucket=self.bucket_name, Body=body, Key=filepath)

    def read(self, filepath: str):
        obj = self.client.get_object(Bucket=self.bucket_name, Key=filepath)
        return obj['Body'].read()
