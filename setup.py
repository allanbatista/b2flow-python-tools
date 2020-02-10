from setuptools import setup

setup(
    name='b2flow-python-tools',
    version='0.0.1',
    packages=['b2flow', 'b2flow.python', 'b2flow.python.tools'],
    url='',
    license='MIT',
    author='Allan Batista',
    author_email='allan@allanbatista.com.br',
    description='',
    install_requires=[
        'boto3',
        'pyyaml',
        'pandas',
        'numpy'
    ]
)
