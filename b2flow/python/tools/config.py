import os
import yaml
import sys
import logging


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.realpath(__file__), "../../../../"))
ENVIRONMENT = os.environ.get('ENVIRONMENT') or 'development'


def join(path):
    return os.path.join(PROJECT_ROOT, path)


with open(join("config.yml"), 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader) or {}


def get(key):
    return os.environ.get(key) or str(config[key])


LOG_LEVEL = logging.getLevelName(get('B2FLOW__PYTHON__TOOLS__LOG_LEVEL'))

logger = logging.Logger("default")
logger.setLevel(LOG_LEVEL)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(LOG_LEVEL)

