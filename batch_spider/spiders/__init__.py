# coding:utf8
"""
爬虫模版
"""
# 貌似这里有内存泄漏问题
from gevent import monkey  # isort:skip

# tips  注意 如果 patch了subprocess 会导致 os.popen特别慢...
monkey.patch_all(os=False, subprocess=False, signal=False)  # isort:skip

from batch_spider.network.sample_request import Request
from batch_spider.network.sample_response import Response

from .base import Spider
from .batch_spider import BatchSpider, JsonTask, SingleBatchSpider  # noqa

__all__ = [
    "BatchSpider",
    "Spider",
    "Request",
    "Response",
    "SingleBatchSpider",
    "JsonTask",
]
