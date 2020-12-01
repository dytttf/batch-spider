# coding:utf8
from setuptools import setup, find_packages

version = "0.1.0"

setup(
    name="batch-spider",
    version=version,
    description="A Spider FrameWork for Batch.",
    long_description=open("README.MD").read(),
    author="Dytttf",
    author_email="duanchiyigaofei@gmail.com",
    url="https://github.com/dytttf/batch-spider",
    packages=find_packages(),
    install_requires=[
        "requests",
        "gevent",
        "pymysql",
        "redis>=3.0.0",
        "requests-ftp",
        "better-exceptions",
        "tqdm",
    ],
    license="BSD",
    classifiers=(),
    keywords=["batch-spider"],
)
