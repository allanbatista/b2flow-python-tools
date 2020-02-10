import os
import yaml

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.realpath(__file__), "../../../../"))
ENVIRONMENT = os.environ.get('ENVIRONMENT') or 'development'


def join(path):
    return os.path.join(PROJECT_ROOT, path)


with open(join("config.yml"), 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader) or {}


def get(key):
    return os.environ.get(key) or str(config[key])