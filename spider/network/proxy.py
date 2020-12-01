# coding:utf8
"""
代理池

1、有限代理


2、无限代理
    无长度限制

"""


from spider import setting
from spider.utils import log

logger = log.get_logger(__file__)

from spider.share.network.proxy import *


# 默认代理池
default_proxy_pool = ProxyPool(
    size=-1, proxy_source_url=setting.get_proxy_uri(), logger=logger
)

if __name__ == "__main__":
    pass
